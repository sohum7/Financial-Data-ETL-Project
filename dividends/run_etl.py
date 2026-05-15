# Run ETL Pipeline

# Built-in imports
import hashlib
from http import HTTPStatus

# ETL imports
from etl.extract.extractor import extract as extract
from etl.transform.transformer import transform
from etl.load.loader import load
from etl.load.merger import merge

# Shared imports
from shared.clients.gcp.logging import GCPLogger
from shared.clients.gcp.services import create_dataset, check_blob_exists, read_json_gcs, write_json_gcs, read_parquet_gcs, write_parquet_gcs
from shared.clients.gcp.naming_conv import MS_FILE_NM, GCSPathLib
from shared.configs.config_loader import main as run_config_main; run_config_main();
from shared.configs.config_loader import MS_CAT, MS_BASE_URL_V2, MS_SYMBOLS_LST, MS_DATA_CTGYS_LST, MS_V2_API_KEY, MS_TGT_DATASET_NM, MS_TGT_TBL_NM, MS_STG_DATASET_NM, MS_STG_TBL_NM, MS_RAW_FILE_BUCKET_NM,  MS_RAW_FILE_BUCKET_DIR, MS_RAW_FILE_TYPE, MS_TFD_FILE_BUCKET_NM,  MS_TFD_FILE_BUCKET_DIR, MS_TFD_FILE_TYPE
from shared.misc.utilities import http_return, get_past_week_range, dict_to_logs

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
    estimated_row_cnt = len(symbols_lst) * 5
    
    # get last processed date for the data category from the database
    # TODO
    
    # modify the date parameters to be in string format for the API request
    symbols_lst.sort() # Sort the symbols list for consistent ordering
    symbols_lst_str = ",".join(symbols_lst) # Convert the symbols list to a comma-separated string for the API request
    
    hash_input = f"{MS_CAT}_{start_dt}_{end_dt}_{symbols_lst_str}" # Create a unique hash input string based on the data category, date range, and symbols list for consistent hashing
    hash_output = hashlib.sha256(hash_input.encode()).hexdigest()[:max_hash_len]
    
    wkly_subdir = f"start_dt={start_dt}"
    file_nm = MS_FILE_NM(MS_CAT, start_dt, end_dt, hash_output)
    
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
                            hash_output: {hash_output} \
                            file_nm: {file_nm} \
                            wkly_subdir: {wkly_subdir} \
                            ")
        #############################################    ETL PROCESS STARTING    #############################################
        # EXTRACTION STARTING
        gcp_logger.info("Starting extraction process...")
        
        
        
        raw_gcs_path_obj = GCSPathLib(MS_RAW_FILE_BUCKET_NM, f"{MS_RAW_FILE_BUCKET_DIR.strip('/')}/{wkly_subdir}", file_nm, MS_RAW_FILE_TYPE)
        
        raw_json, extract_metrics = extract(MS_CAT, MS_BASE_URL_V2, symbols_lst_str, MS_V2_API_KEY, raw_gcs_path_obj, batch_dt, start_dt, end_dt, full_refresh, gcp_logger)
        
        
        
        gcp_logger.info("******** Extraction process completed ********")
        # EXTRACTION SUCCEEDED
        #######################################################################################################################
        # TRANSFORMATION STARTING
        gcp_logger.info("Starting transformation process...")
        
        
        
        tfd_gcs_path_obj = GCSPathLib(MS_TFD_FILE_BUCKET_NM, f"{MS_TFD_FILE_BUCKET_DIR.strip('/')}/{wkly_subdir.strip('/')}", file_nm, MS_TFD_FILE_TYPE)
        
        tfd_df, transform_metrics = transform(raw_json, tfd_gcs_path_obj, full_refresh, gcp_logger)
        
        
        
        gcp_logger.info("********  Transformation process completed  ********")    
        # TRANSFORMATION SUCCEEDED
        #######################################################################################################################
        # LOADING STARTING
        gcp_logger.info("Starting loading process...")
        
        
        
        tgt_ds_tbl = f"{MS_TGT_DATASET_NM}.{MS_TGT_TBL_NM}"
        stg_ds_tbl = f"{MS_STG_DATASET_NM}.{MS_STG_TBL_NM}"
        
        create_dataset(MS_TGT_DATASET_NM, "US", gcp_logger)
        create_dataset(MS_STG_DATASET_NM, "US", gcp_logger)
        
        
        
        load(tfd_df, tgt_ds_tbl, stg_ds_tbl, logger=gcp_logger)
        
        
        
        gcp_logger.info("Starting merging process...")
        merge(tgt_ds_tbl, stg_ds_tbl, logger=gcp_logger)
        gcp_logger.info(f"SUCCESS: Merged from {stg_ds_tbl} to {tgt_ds_tbl}")
        
        
        gcp_logger.info("********  Load process completed  ********")
        # LOADING SUCCEEDED
        #############################################    ETL PROCESS SUCCEEDED    #############################################
        
        # Row count logging
        dict_to_logs(extract_metrics, gcp_logger)
        dict_to_logs(transform_metrics, gcp_logger)
        
        """
        gcp_logger.info(f"estimated_row_cnt:   {estimated_row_cnt}")
        gcp_logger.info(f"raw_json_row_cnt:    {raw_json_row_cnt}")
        gcp_logger.info(f"tfd_df_row_cnt:      {tfd_df_row_cnt}")
        gcp_logger.info(f"load_tbl_cnt:        VAR UNDEFINED")
        gcp_logger.info(f"merge_tbl_prior_cnt: VAR UNDEFINED")
        gcp_logger.info(f"merge_tbl_after_cnt: VAR UNDEFINED")
        gcp_logger.info("NOTE: estimated_row_cnt does not take into account for holidays/non-market dates")
        """
        
        gcp_logger.info(f"********  ETL process for {data_cat.upper()} completed  ********")


if __name__ == "__main__":
    full_refresh = True
    
    kwargs: dict[str, dict[str, str]] = {
        "manual_override_dates": {
            "batch_dt": "2026-01-31",
            "start_dt": "2025-01-01",
            "end_dt"  : "2025-12-31"
        }
    }
    
    run_pipeline("dividends", full_refresh, **kwargs)