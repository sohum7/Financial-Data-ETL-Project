# Extractor module for dividends ETL pipeline. Contains functions related to the extraction step of the ETL pipeline, which involves extracting raw data from the Microsoft API and saving it to a GCS bucket for further processing in the transformation and loading steps of the ETL pipeline.

# Built-in imports
from logging import Logger

# Shared imports
from shared.clients.gcp.gcs import FileConfig
from shared.clients.gcp.logging import CloudLogger
from shared.clients.gcp.services import check_blob_exists, read_json, write_json
from shared.clients.ms.api import APIConfig
from shared.clients.ms.services import ms_api_request


# Extraction entry point
def extract(ms_api_obj: APIConfig, gcs_path_obj: FileConfig, full_refresh: bool, logger: Logger | CloudLogger) -> tuple[list[dict[str, str]], dict[str, str | int]]:
    """Extract json via Market Stack API with caching functionality

    Args:
        ms_api_obj (APIConfig): Contains necessary Market Stack data in order to perform a API request for data extraction
        gcs_path_obj (FileConfig): Contains necessary Google Cloud Storage data in order to save or retrieve object blobs
        full_refresh (bool): Bypasses cached data extraction files and performs API extraction
        logger (Logger | CloudLogger): Utilized for logging steps taken as well as errors

    Returns:
        tuple[list[dict[str, str]], dict[str, str | int]]: Data portion of JSON file + metrics
    """
    
    metrics = {}
    bucket_nm = gcs_path_obj.bucket_nm
    blob_nm = gcs_path_obj.blob_nm
    
    # Check if raw blob file exists in GCS bucket
    raw_file_exists = check_blob_exists(bucket_nm, blob_nm)
    metrics["raw_blob_exists"] = raw_file_exists
    
    logger.info(f"RUNNING: Extraction of {ms_api_obj.data_type.upper()} data...")
    
    # If raw file exists and full_refresh is False, read the raw file from GCS ie caching
    # Otherwise, make MS API request call to extract raw data and save/replace it to GCS.
    if raw_file_exists and not full_refresh:
        raw_json = read_raw_json(gcs_path_obj, logger)
        metrics["raw_blob_path"] = gcs_path_obj.blob_path
    else:
        raw_json = extract_main(ms_api_obj, logger)
        metrics["raw_blob_path"] = write_raw_json(raw_json, gcs_path_obj, logger)
    
    logger.info("SUCCESS: Extraction complete.")
    
    metrics["raw_data_row_cnt"] = len(raw_json["data"])
    return raw_json["data"], metrics

# Extraction of Market Stack data via API Request
def extract_main(ms_api_obj: APIConfig, logger: Logger | CloudLogger) -> dict:
    """Extract json via Market Stack API

    Args:
        ms_api_obj (APIConfig): Contains necessary Market Stack data in order to perform a API request for data extraction
        logger (Logger | CloudLogger): Utilized for logging steps taken as well as errors

    Returns:
        dict: Extracted JSON from Market Stack API ..... + TODO: add metrics?????
    """
    
    logger.info("Starting the main extraction process...")
    logger.info(ms_api_obj)
    
    # Make the Market Stack API call
    try:
        raw_json = ms_api_request(ms_api_obj.data_type, ms_api_obj.base_url, ms_api_obj.symbols_str, ms_api_obj.api_key, ms_api_obj.start_dt, ms_api_obj.end_dt)
    except Exception as e:
        err_msg = "Failed to extract raw data via MS API request call"
        logger.error(f"ERROR: {err_msg}: {e}")
        raise ValueError(err_msg) 
    
    # Ensure the data field is present in the json
    if "data" not in raw_json:
        err_msg = "Key not found."
        err_reason = "Raw file does not contain 'data' key."
        logger.error(f"ERROR: {err_msg} - REASON: {err_reason}")
        raise KeyError(err_msg)
    
    logger.info("Raw data was extracted via MS API.")
    
    return raw_json

# Read the raw extracted json from GCS bucket
def read_raw_json(gcs_path_obj: FileConfig, logger: Logger | CloudLogger) -> dict:
    """Read extracted json file from GCS bucket

    Args:
        gcs_path_obj (FileConfig): _description_
        logger (Logger | CloudLogger): Utilized for logging steps taken as well as errors

    Returns:
        dict: _description_
    """
    
    logger.info("Reading raw file from GCS bucket...")
    logger.info(gcs_path_obj)
    
    # Read json from GCS bucket
    try:
        raw_json = read_json(gcs_path_obj.bucket_nm, gcs_path_obj.blob_nm)
    except Exception as e:
        err_msg = "Failed to read raw file from GCS"
        logger.error(f"ERROR: {err_msg}: {e}")
        raise ValueError(err_msg) 
    
    logger.info("Raw file was read.")
    
    return raw_json

# Write the raw extracted json from GCS bucket
def write_raw_json(raw_json: dict, gcs_path_obj: FileConfig, logger: Logger | CloudLogger) -> str:
    """Write extracted json file to GCS bucket

    Args:
        raw_json (dict): _description_
        gcs_path_obj (FileConfig): _description_
        logger (Logger | CloudLogger): Utilized for logging steps taken as well as errors

    Returns:
        str: _description_
    """
    
    logger.info("Writing raw file to GCS bucket...")
    logger.info(gcs_path_obj)
    
    # Write json to GCS bucket
    try:
        blob_path = write_json(raw_json, gcs_path_obj.bucket_nm, gcs_path_obj.blob_nm)
    except Exception as e:
        err_msg = "Failed to write raw file to GCS"
        logger.error(f"ERROR: {err_msg}: {e}")
        raise ValueError(err_msg) 
    
    logger.info("Raw file was written.")
    return blob_path
