
# Configuration file for data extractionm transformation, loading, and storage parameters for Market Stack data. 
# This file defines constants for API keys, URLs, file formats, and GCS bucket details used across the ETL pipeline.

# Builtin imports
from configparser import ConfigParser
from dotenv import load_dotenv
from os import getenv as os_getenv
from pathlib import Path


def load_config():
    BASE_DIR = Path(__file__).resolve().parent
    
    load_dotenv()
    
    config = ConfigParser()
    config.read(BASE_DIR / "config.ini") # Load base first
    
    env_vars = {
        "ENVIRONMENT": os_getenv("ENVIRONMENT", ""),
        "BUCKET_SUFFIX": os_getenv("BUCKET_SUFFIX", ""),
        "GOOGLE_CLOUD_PROJECT": os_getenv("GOOGLE_CLOUD_PROJECT", "")
    }
    
    if not env_vars["GOOGLE_CLOUD_PROJECT"]:
        raise ValueError("missing required environment variable: GOOGLE_CLOUD_PROJECT")
    if not env_vars["ENVIRONMENT"]:
        raise ValueError("missing required environment variable: ENV")
    
    return config, env_vars

def main():
    # Load configuration and environment variables
    global config, env_vars
    config, env_vars = load_config()

# Run main function to set global variables
main()

# Set variables based on config and environment variables
gc_project = env_vars["GOOGLE_CLOUD_PROJECT"]
gc_env = env_vars["ENVIRONMENT"]

ms_cfg = config["MARKET_STACK_METADATA"]
ms_div_cfg = config["MARKET_STACK_DIVIDENDS_METADATA"]

MS_SYMBOLS = [symbol.strip() for symbol in ms_cfg["symbols"].split(",")]
MS_BASE_URL = ms_cfg["base_url"]

MS_CAT = ms_div_cfg["name"]
#MS_DIV_URL = f"{MS_BASE_URL}{MS_CAT}/"

## Extract source data
MS_DIV_RAW_FILE_TYPE = ms_div_cfg["raw_file_type"]
MS_DIV_RAW_FILE_BUCKET_NM = f"{ms_div_cfg['raw_file_bucket_base']}-{gc_env}"
MS_DIV_RAW_FILE_BUCKET_SUBDIR = ms_div_cfg["raw_file_bucket_subdir"]

## Transformed data
MS_DIV_TFD_FILE_TYPE = ms_div_cfg["tfd_file_type"]
MS_DIV_TFD_FILE_BUCKET_NM = f"{ms_div_cfg['tfd_file_bucket_base']}-{gc_env}"
MS_DIV_TFD_FILE_BUCKET_SUBDIR = ms_div_cfg["tfd_file_bucket_subdir"]

## Cleaned data
MS_DIV_CLN_FILE_TYPE = ms_div_cfg["cln_file_type"]
MS_DIV_CLN_FILE_BUCKET_NM = f"{ms_div_cfg['cln_file_bucket_base']}-{gc_env}"
MS_DIV_CLN_FILE_BUCKET_SUBDIR = ms_div_cfg["cln_file_bucket_subdir"]
