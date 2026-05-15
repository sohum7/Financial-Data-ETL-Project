# 

# Shared imports
from shared.clients.gcp.naming_conv import GCSPathLib
from shared.clients.gcp.services import check_blob_exists, read_json_gcs, write_json_gcs
from shared.clients.ms.ms_api import ms_api_request


def extract(ms_data_cat: str, ms_base_url: str, ms_symbols_lst_str: str, ms_api_key: str, gcs_path_obj: GCSPathLib, batch_dt: str, start_dt: str, end_dt: str, full_refresh: bool, logger) -> tuple[list[dict], dict[str, str]]:    
    metrics = {}
    bucket_nm, blob_nm = gcs_path_obj.bucket_blob_nms()
    
    logger.info(f"Starting extraction of {ms_data_cat}")
    
    # Check if raw file already exists in GCS bucket. If it does, read it instead of making MS API request call.
    raw_file_exists = check_blob_exists(bucket_nm, blob_nm, logger)
    metrics["file_exists"] = raw_file_exists
    
    # If raw file exists and full_refresh is False, read the raw file from GCS. Otherwise, make MS API request call to extract raw data and save/replace it to GCS.
    raw_json: dict = \
        read_json_from_gcs(bucket_nm, blob_nm, logger) \
        if raw_file_exists and not full_refresh else  \
        extract_json_ms_api(ms_data_cat, ms_base_url, ms_symbols_lst_str, ms_api_key, bucket_nm, blob_nm, batch_dt, start_dt, end_dt, logger) 
    
    if "data" not in raw_json:
        logger.error("ERROR: Raw file does not contain 'data' key.")
        raise ValueError("Raw file does not contain 'data' key.")
    
    metrics["row_cnt"] = len(raw_json["data"])
    
    return raw_json["data"], metrics

def read_json_from_gcs(bucket_nm: str, blob_nm: str, logger):
    logger.info("No need to extract via MS API request call as blob exists.")
        
    # Read the raw file from GCS and return it as a JSON object.
    logger.info("Reading raw file from gcs blob...")
    raw_json = read_json_gcs(bucket_nm, blob_nm, logger)
    
    # Check if raw_json is None, which indicates that the file was not read successfully from GCS.
    if raw_json is None:
        err_msg = "ERROR: Raw file was not read."
        logger.error(err_msg)
        raise ValueError(err_msg)
    
    logger.info("SUCCESS: Raw file was read.")
    return raw_json

def extract_json_ms_api(data_cat: str, base_url: str, symbols_lst_str: str, api_key: str, bucket_nm: str, blob_nm: str, batch_dt: str, start_dt: str, end_dt: str, logger):
    # Make MS API request call to extract raw data.
    logger.info("Making MS API request call...")
    raw_json = ms_api_request(data_cat, base_url, symbols_lst_str, api_key, batch_dt, start_dt, end_dt, logger)

    # Check if raw_json is None, which indicates that the MS API request call was not successful in extracting raw data.
    if raw_json is None:
        err_msg = "ERROR: Raw data was unable to be extracted via MS API request call."
        logger.error(err_msg)
        raise ValueError(err_msg)

    logger.info("SUCCESS: Raw data was extracted.")
    
    # Save/replace the raw data to GCS.
    logger.info("Starting to save raw to GCS...")
    raw_file_path = write_json_gcs(raw_json, bucket_nm, blob_nm, logger)
    
    # Check if raw_file_path is None, which indicates that the raw data was not saved successfully to GCS.
    if raw_file_path is None:
        err_msg = "ERROR: Raw data was not saved to GCS."
        logger.error(err_msg)
        raise ValueError(err_msg)
    
    logger.info(f"SUCCESS: Raw data saved to: {raw_file_path}")
    return raw_json