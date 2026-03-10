#

# Builtin imports
from datetime import datetime
import hashlib

# Shared imports
from etl.extract.extractor import extract as extract_run
from etl.transform.transformer import transform as transform_run
from etl.load.loader import load as load_run
from shared.clients.gcp_logging import GCPLogger
from shared.clients.gcp_services import write_json_to_gcs, write_df_to_gcs
from shared.clients.gcp_gcs_naming import DF_SAVE_PATH
from shared.misc.utilities import http_return, getCurWkDtRange
from shared.configs.config_loader import MS_CAT, MS_SYMBOLS_LST, MS_TGT_DATASET_NM, MS_TGT_TBL_NM, MS_STG_DATASET_NM, MS_STG_TBL_NM, MS_RAW_FILE_BUCKET_NM,  MS_RAW_FILE_BUCKET_DIR, MS_RAW_FILE_TYPE, MS_TFD_FILE_BUCKET_NM,  MS_TFD_FILE_BUCKET_DIR, MS_TFD_FILE_TYPE
from shared.clients.gcp_gcs_naming import MS_FILE_NM



def run_pipeline():
    # Define parameters for the ETL process
    symbols_lst = MS_SYMBOLS_LST # Sort the symbols list for consistent ordering
    batch_dt, start_dt, end_dt = getCurWkDtRange()
    
    # get last processed date for the data category from the database
    # TODO
    
    # modify the date parameters to be in string format for the API request
    symbols_lst.sort() # Sort the symbols list for consistent ordering
    symbols_lst_str = ",".join(symbols_lst) # Convert the symbols list to a comma-separated string for the API request
    
    hash_input = f"{MS_CAT}_{start_dt}_{end_dt}_{symbols_lst_str}" # Create a unique hash input string based on the data category, date range, and symbols list for consistent hashing
    file_hash = hashlib.md5(hash_input.encode()).hexdigest()
    sub_dir = f"{start_dt}_{end_dt}_{file_hash}/"
    
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
        gcp_logger.info("Starting extraction process...")
        raw_json = extract_run(MS_CAT, symbols_lst_str, batch_dt, start_dt, end_dt, logger=gcp_logger)
        
        if raw_json is None:
            gcp_logger.error("Extraction failed. No data returned from API.")
            return http_return(500, "Extraction failed. No data returned from API.")
        # else
        gcp_logger.info("Extraction process completed.")
        
        
        
        
        # This will be used later for preventing process reruns, reducing compute and cost ie chacheing
        gcp_logger.info("Starting to save raw JSON to GCS...")
        file_type = "json"
        file_nm = MS_FILE_NM(MS_CAT, start_dt, end_dt, file_hash)
        raw_json_file_path = write_json_to_gcs(raw_json, MS_RAW_FILE_BUCKET_NM, MS_RAW_FILE_BUCKET_DIR, file_nm, batch_dt, start_dt, end_dt)
        
        if raw_json_file_path is None:
            gcp_logger.error("Raw file not saved")
            return http_return(500, "Raw file not saved")
        
        
        
        
        # Call the transform function to get the transformed DataFrame
        gcp_logger.info("Starting transformation process...")
        df = transform_run(raw_json, gcp_logger)
        
        if df is None:
            gcp_logger.error("Transformation failed. No DataFrame returned.")
            return http_return(500, "Transformation failed. No DataFrame returned.")
        # else
        gcp_logger.info("Transformation process completed.")
        
        
        
        
        gcp_logger.info("Starting to save transformed DataFrame to GCS...")
        week_specific_dir = f"{MS_TFD_FILE_BUCKET_DIR}{'' if MS_TFD_FILE_BUCKET_DIR.endswith('/') else '/'}{sub_dir}"
        parquet_file_path = write_df_to_gcs(df, MS_TFD_FILE_BUCKET_NM, week_specific_dir, partition_col="symbol", cluster_col="market_dt", file_type=file_type, save_mode="append")
        if parquet_file_path is None:
            gcp_logger.error("Transformed files not saved")
            return http_return(500, "Transformed files not saved")
        
        
        
        
        
        # Call the load function to load the transformed data to the destination
        gcp_logger.info("Starting load process...")
        bucket_dir = DF_SAVE_PATH(MS_TFD_FILE_BUCKET_NM, week_specific_dir)
        tgt_ds_tbl = f"{MS_TGT_DATASET_NM}.{MS_TGT_TBL_NM}"
        stg_ds_tbl = f"{MS_STG_DATASET_NM}.{MS_STG_TBL_NM}"
        lr_res = load_run(MS_CAT, bucket_dir, tgt_ds_tbl, stg_ds_tbl, MS_TFD_FILE_TYPE, gcp_logger)
        
        if not lr_res:
            gcp_logger.error("Load failed.")
            return http_return(500, "Load failed.")
        # else
        gcp_logger.info("Load process completed.")
        


if __name__ == "__main__":
    run_pipeline()