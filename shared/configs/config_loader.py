
# Configuration file for data extractionm transformation, loading, and storage parameters for Market Stack data. 
# This file defines constants for API keys, URLs, file formats, and GCS bucket details used across the ETL pipeline.

# Builtin imports
from configparser import ConfigParser
from dotenv import load_dotenv
from os import getenv as os_getenv
from pathlib import Path
from shared.clients.gcp_services import get_secret


def load_config():
    BASE_DIR = Path(__file__).resolve().parent
    
    load_dotenv()
    
    config = ConfigParser()
    config.read(BASE_DIR / "config.ini") # Load base first
    
    env_vars = {
        "ENVIRONMENT": os_getenv("ENVIRONMENT", ""),
        "BUCKET_SUFFIX": os_getenv("BUCKET_SUFFIX", ""),
        "PROJECT_ID": os_getenv("PROJECT_ID", "")
    }
    
    if not env_vars["PROJECT_ID"]:
        raise ValueError("missing required environment variable: PROJECT_ID")
    if not env_vars["ENVIRONMENT"]:
        raise ValueError("missing required environment variable: ENV")
    
    return config, env_vars

def load_cat_config(data_cat):
    pass
def main():
    # Load configuration and environment variables
    global config, env_vars
    config, env_vars = load_config()

# Run main function to set global variables
main()

# Set variables based on config and environment variables
gc_project = env_vars["PROJECT_ID"]
gc_env = env_vars["ENVIRONMENT"]

ms_cfg = config["MARKET_STACK_METADATA"]
ms_cat_cfg = config["MARKET_STACK_DIVIDENDS_METADATA"]

MS_SYMBOLS_LST = [symbol.strip() for symbol in ms_cfg["symbols"].split(",")]
MS_BASE_URL = ms_cfg["base_url"]

MS_CAT = ms_cat_cfg["name"]
MS_V2_API_KEY = get_secret("MS_V2_API_KEY")

## Extract source data
MS_RAW_FILE_TYPE = ms_cat_cfg["raw_file_type"]
MS_RAW_FILE_BUCKET_NM = f"{ms_cat_cfg['raw_file_bucket_base']}-{gc_env}"
MS_RAW_FILE_BUCKET_DIR = ms_cat_cfg["raw_file_bucket_dir"]

## Transformed data
MS_TFD_FILE_TYPE = ms_cat_cfg["tfd_file_type"]
MS_TFD_FILE_BUCKET_NM = f"{ms_cat_cfg['tfd_file_bucket_base']}-{gc_env}"
MS_TFD_FILE_BUCKET_DIR = ms_cat_cfg["tfd_file_bucket_dir"]

## Cleaned data location
MS_TGT_DATASET_NM = f"{ms_cfg['bq_target_dataset_base']}-{gc_env}"
MS_STG_DATASET_NM = f"{ms_cfg['bq_staging_dataset_base']}-{gc_env}"
MS_TGT_TBL_NM = MS_CAT
MS_STG_TBL_NM = f"{MS_TGT_TBL_NM}_staging"
