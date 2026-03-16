# Run ETL Pipeline

# Built-in imports
from datetime import datetime
import hashlib
from http import HTTPStatus

# ETL imports
from etl.extract.extractor import extract as extract_run
from etl.transform.transformer import transform as transform_run
from etl.load.loader import load as load_run
from etl.load.merger import merge as merge_run

# Shared imports
from shared.clients.gcp.logging import GCPLogger
from shared.clients.gcp.services import create_dataset, check_blob_exists, read_json_gcs, write_json_gcs, read_parquet_gcs, write_parquet_gcs, convert_dict_pandas_df
from shared.clients.gcp.naming_conv import MS_FILE_NM, GCSPathLib
from shared.configs.config_loader import main as run_config_main; run_config_main();
from shared.configs.config_loader import MS_CAT, MS_CAT_URL, MS_SYMBOLS_LST, MS_DATA_CTGYS_LST, MS_V2_API_KEY, MS_TGT_DATASET_NM, MS_TGT_TBL_NM, MS_STG_DATASET_NM, MS_STG_TBL_NM, MS_RAW_FILE_BUCKET_NM,  MS_RAW_FILE_BUCKET_DIR, MS_RAW_FILE_TYPE, MS_TFD_FILE_BUCKET_NM,  MS_TFD_FILE_BUCKET_DIR, MS_TFD_FILE_TYPE
from shared.misc.utilities import http_return, get_past_week_range

# Defaults
HTTP_OK_CODE = HTTPStatus.OK.value
HTTP_SERVER_ERR_CODE = HTTPStatus.INTERNAL_SERVER_ERROR.value


def run_pipeline(data_cat, full_refresh=False, **kwargs):
    max_hash_len=16
    batch_dt: str
    start_dt: str
    end_dt: str
    if "manual_override_dates" in kwargs and kwargs["manual_override_dates"]:
        batch_dt, start_dt, end_dt = kwargs["manual_override_dates"]["batch_dt"], kwargs["manual_override_dates"]["start_dt"], kwargs["manual_override_dates"]["end_dt"]
    else:
        batch_dt, start_dt, end_dt = get_past_week_range()
    
    
    if data_cat not in MS_DATA_CTGYS_LST:
        raise Exception(f"{data_cat} is not an approved")
    # Define parameters for the ETL process
    symbols_lst = MS_SYMBOLS_LST # Sort the symbols list for consistent ordering
    
    # get last processed date for the data category from the database
    # TODO
    
    # modify the date parameters to be in string format for the API request
    symbols_lst.sort() # Sort the symbols list for consistent ordering
    symbols_lst_str = ",".join(symbols_lst) # Convert the symbols list to a comma-separated string for the API request
    
    hash_input = f"{MS_CAT}_{start_dt}_{end_dt}_{symbols_lst_str}" # Create a unique hash input string based on the data category, date range, and symbols list for consistent hashing
    file_hash = hashlib.sha256(hash_input.encode()).hexdigest()[:max_hash_len]
    
    wkly_subdir = f"start_dt={start_dt}"
    file_nm = MS_FILE_NM(MS_CAT, start_dt, end_dt, file_hash)
    
    # check if already processed data
    #       check if fle already exists in GCS for the given date range and data category, if yes then skip the extraction and transformation steps and proceed to load step, if no then proceed with the extraction and transformation steps. we can also choose to overwrite the existing file in GCS if it already exists, but for now we will just skip the extraction and transformation steps if the file already exists in GCS since we want to avoid unnecessary API calls and transformations if we already have the data for the given date range and data category in GCS. we can implement the logic to check if the file already exists in GCS in the load step since it is more efficient to check for file existence in GCS during the load step when we are about to load the transformed data to GCS rather than checking for file existence in GCS during the extract or transform steps before we have even transformed the data since it would require additional API calls to GCS to check for file existence before we have even transformed the data which would be inefficient and unnecessary if we end up not needing to transform the data at all because we already have it in GCS for the given date range and data category.
    # extract
    #       market api call
    # save raw to gcs
    #       save the raw JSON response from the API call to GCS for the given date range and data category, we can save the raw JSON response to GCS in a separate folder or bucket for raw data with a file naming convention that includes the data category and date range for easy identification and retrieval later on if needed. we can also choose to not save the raw JSON response to GCS if we want to save on storage costs and we are confident that we can always re-extract the data from the API if needed in the future, but for now we will save the raw JSON response to GCS for the given date range and data category for better traceability and debugging purposes in case we need to go back and check the raw data later on for any issues or discrepancies.
    # transform
    #       clean and transform data with df operations
    # check if already processed data
    #       check if fle already exists in GCS for the given date range and data category, if yes then skip the extraction and transformation steps and proceed to load step, if no then proceed with the extraction and transformation steps. we can also choose to overwrite the existing file in GCS if it already exists, but for now we will just skip the extraction and transformation steps if the file already exists in GCS since we want to avoid unnecessary API calls and transformations if we already have the data for the given date range and data category in GCS. we can implement the logic to check if the file already exists in GCS in the load step since it is more efficient to check for file existence in GCS during the load step when we are about to load the transformed data to GCS rather than checking for file existence in GCS during the extract or transform steps before we have even transformed the data since it would require additional API calls to GCS to check for file existence before we have even transformed the data which would be inefficient and unnecessary if we end up not needing to transform the data at all because we already have it in GCS for the given date range and data category.
    # load
    #       write to bq staging table
    # merge
    #   merge from staging table to main table
    
    
    with GCPLogger() as gcp_logger:
        gcp_logger.info(  f"batch_dt: {batch_dt} \
                            start_dt: {start_dt} \
                            end_dt: {end_dt} \
                            symbols_lst_str: {symbols_lst_str} \
                            hash_input: {hash_input} \
                            file_hash: {file_hash} \
                            file_nm: {file_nm} \
                            wkly_subdir: {wkly_subdir} \
                            ")
        ####################    ETL PROCESS STARTING    ####################
        # EXTRACTION STARTING
        
        gcp_logger.info("Starting extraction process...")
        
        raw_gcs_path_obj = GCSPathLib(MS_RAW_FILE_BUCKET_NM, f"{MS_RAW_FILE_BUCKET_DIR.strip('/')}/{wkly_subdir}", file_nm, MS_RAW_FILE_TYPE)
        raw_blob_path, raw_blob_nm, raw_bucket_nm, _, _ = raw_gcs_path_obj.getVars()
        
        raw_json = None
        raw_file_exists = check_blob_exists(raw_bucket_nm, raw_blob_nm)
        
        gcp_logger.info(f"raw_file_exists: {raw_file_exists}")
        
        if raw_file_exists:
            gcp_logger.info(f"Raw source file already exists: {raw_blob_path}\nNo need for MS API request call.")
            
            gcp_logger.info(f"Reading raw {MS_RAW_FILE_TYPE} file from gcs blob...")
            raw_json = read_json_gcs(raw_bucket_nm, raw_blob_nm)
            
            if raw_json is None:
                err_msg = f"ERROR: Raw {MS_RAW_FILE_TYPE} file was not read."
                gcp_logger.error(err_msg)
                return http_return(HTTP_SERVER_ERR_CODE, err_msg)
            
            gcp_logger.info(f"SUCCESS: Raw {MS_RAW_FILE_TYPE} file was read.") # i can push these message to a list and send the list all at once????
        else:
            gcp_logger.info("Making MS API request call...")
            raw_json = extract_run(MS_CAT_URL, symbols_lst_str, MS_V2_API_KEY, batch_dt, start_dt, end_dt, logger=gcp_logger)
            
            if raw_json is None:
                err_msg = "ERROR: Raw source data was unable to be extracted via MS API request call."
                gcp_logger.error(err_msg)
                return http_return(HTTP_SERVER_ERR_CODE, err_msg)
            
            gcp_logger.info("SUCCESS: Raw source data was extracted.")
            
            gcp_logger.info(f"Starting to save raw {MS_RAW_FILE_TYPE} to GCS...")
            raw_file_path = write_json_gcs(raw_json, raw_bucket_nm, raw_blob_nm)
            
            if raw_file_path is None:
                err_msg = "ERROR: Raw data not saved to GCS."
                gcp_logger.error(err_msg)
                return http_return(HTTP_SERVER_ERR_CODE, err_msg)
            
            gcp_logger.info(f"SUCCESS: Raw data saved to: {raw_file_path}")
            
        if raw_json is None:
            err_msg = "ERROR: Extraction failed and/or no data returned from API."
            gcp_logger.error(err_msg)
            return http_return(HTTP_SERVER_ERR_CODE, err_msg)
        
        gcp_logger.info("******** Extraction process completed ********")
        
        # EXTRACTION SUCCEEDED
        #######################################################################################################################
        # TRANSFORMATION STARTING
        
        gcp_logger.info("Starting transformation process...")
        
        tfd_gcs_path_obj = GCSPathLib(MS_TFD_FILE_BUCKET_NM, f"{MS_TFD_FILE_BUCKET_DIR.strip('/')}/{wkly_subdir.strip('/')}", file_nm, MS_TFD_FILE_TYPE)
        tfd_blob_path, tfd_blob_nm, tfd_bucket_nm, _, _ = tfd_gcs_path_obj.getVars()
        
        tfd_df = None
        tfd_file_exists = check_blob_exists(tfd_bucket_nm, tfd_blob_nm)
        
        gcp_logger.info(f"tfd_file_exists: {tfd_file_exists}")
        
        if tfd_file_exists:
            gcp_logger.info(f"File already exists: {tfd_blob_path}\nNo need to transform MS data again.")
            
            gcp_logger.info(f"Reading {MS_TFD_FILE_TYPE} file from gcs blob...")
            tfd_df = read_parquet_gcs(tfd_bucket_nm, tfd_blob_nm)
            
            if tfd_df is None:
                err_msg = f"ERROR: Transformed {MS_TFD_FILE_TYPE} file was not read."
                gcp_logger.error(err_msg)
                return http_return(HTTP_SERVER_ERR_CODE, err_msg)
            
            gcp_logger.info(f"SUCCESS: Transformed {MS_TFD_FILE_TYPE} file was read.")
        else:
            gcp_logger.info("Transforming df...")
            raw_df = convert_dict_pandas_df(raw_json, "data", gcp_logger)
            tfd_df = transform_run(raw_df, logger=gcp_logger)
            
            if tfd_df is None:
                err_msg = "ERROR: Transformation failed."
                gcp_logger.error(err_msg)
                return http_return(HTTP_SERVER_ERR_CODE, err_msg)
            
            gcp_logger.info(f"SUCCESS: Transformation completed.")
            gcp_logger.info(f"Raw DataFrame row count: {len(raw_df)}\nTransformed DataFrame row count: {len(tfd_df)}")
            
            gcp_logger.info(f"Starting to save transformed data as {MS_TFD_FILE_TYPE} to GCS...")
            tfd_file_path = write_parquet_gcs(tfd_df, tfd_bucket_nm, tfd_blob_nm, partition_cols=None)
            
            if tfd_file_path is None:
                err_msg = "ERROR: Transformed file not saved."
                gcp_logger.error(err_msg)
                return http_return(HTTP_SERVER_ERR_CODE, err_msg)
            
            gcp_logger.info(f"SUCCESS: Transformed file saved to: {tfd_file_path}")
        
        gcp_logger.info("********  Transformation process completed  ********")    
        
        # TRANSFORMATION SUCCEEDED
        #######################################################################################################################
        # LOADING STARTING
        
        gcp_logger.info("Starting loading process...")
        
        tgt_ds_tbl = f"{MS_TGT_DATASET_NM}.{MS_TGT_TBL_NM}"
        stg_ds_tbl = f"{MS_STG_DATASET_NM}.{MS_STG_TBL_NM}"
        
        tgt_ds_exists = create_dataset(MS_TGT_DATASET_NM)
        stg_ds_exists = create_dataset(MS_STG_DATASET_NM)
        
        if not tgt_ds_exists or not stg_ds_exists: 
            err_msg = f"ERROR: Dataset {MS_TGT_DATASET_NM} or {MS_STG_DATASET_NM} was not created."
            gcp_logger.error(err_msg)
            return http_return(HTTP_SERVER_ERR_CODE, err_msg)
        
        lr_res = load_run(tfd_df, tgt_ds_tbl, stg_ds_tbl, logger=gcp_logger)
        
        if not lr_res:
            err_msg = f"ERROR: Load to staging table {stg_ds_tbl} failed."
            gcp_logger.error(err_msg)
            return http_return(HTTP_SERVER_ERR_CODE, err_msg)
        
        gcp_logger.info(f"SUCCESS: Loaded to staging table {stg_ds_tbl}")
        
        gcp_logger.info("Starting merging process...")
        mr_res = merge_run(tgt_ds_tbl, stg_ds_tbl, logger=gcp_logger)
        
        if not mr_res:
            err_msg = f"ERROR: Merge from {stg_ds_tbl} to {tgt_ds_tbl} failed."
            gcp_logger.error(err_msg)
            return http_return(HTTP_SERVER_ERR_CODE, err_msg)
        
        gcp_logger.info(f"SUCCESS: Merged from {stg_ds_tbl} to {tgt_ds_tbl}")
        
        gcp_logger.info("********  Load process completed  ********")
        
        # LOADING SUCCEEDED
        ####################    ETL PROCESS SUCCEEDED    ####################
        
        success_msg = f"********  ETL process for {data_cat.upper()} completed  ********"
        gcp_logger.info(success_msg)
        return http_return(HTTP_OK_CODE, success_msg)


if __name__ == "__main__":
    test = True
    
    kwargs: dict[str, dict[str, str]] = {
        "manual_override_dates": {
            "batch_dt": "2026-03-15",
            "start_dt": "2026-03-09",
            "end_dt"  : "2026-03-13",
        }
    }
    
    run_pipeline("dividends", False, **kwargs)