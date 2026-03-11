
# Configuration file for data extractionm transformation, loading, and storage parameters for Market Stack data. 
# This file defines constants for API keys, URLs, file formats, and GCS bucket details used across the ETL pipeline.

# Builtin imports
from configparser import ConfigParser
from dotenv import load_dotenv
from os import getenv as os_getenv
from pathlib import Path
from google.auth import default as ga_default
from google.cloud import secretmanager as gc_secretmanager


def get_secret(secret_name):
    #project_id = os_getenv("PROJECT_ID")

    _, project_id = ga_default()
    project_id = GC_PROJECT_ID
    client = gc_secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

def load_config() -> tuple[ ConfigParser, dict[ str, str| None]]:
    BASE_DIR = Path(__file__).resolve().parent
    
    load_dotenv()
    
    config = ConfigParser()
    config.read(BASE_DIR / "config.ini") # Load base first
    
    env_vars = {
        "ENVIRONMENT": os_getenv("ENVIRONMENT")
        #,
        #"PROJECT_ID": os_getenv("PROJECT_ID"),
        #"GOOGLE_CLOUD_PROJECT": os_getenv("GOOGLE_CLOUD_PROJECT")'''
    }
    
    #if not env_vars["PROJECT_ID"] and not env_vars["GOOGLE_CLOUD_PROJECT"]:
    #    raise ValueError("missing required environment variable: GOOGLE_CLOUD_PROJECT and PROJECT_ID (only one is needed)")
    if not env_vars["ENVIRONMENT"]:
        raise ValueError("missing required environment variable: ENVIRONMENT")
    
    return config, env_vars


def main(data_cat="dividends"):
    # Load configuration and environment variables
    global config, env_vars, \
        GC_PROJECT_ID, GC_ENV, \
        MS_CAT, MS_CAT_URL, MS_V2_API_KEY, MS_BASE_URL, \
        MS_SYMBOLS_LST, MS_DATA_CTGYS_LST, \
        MS_TGT_DATASET_NM, MS_TGT_TBL_NM, \
        MS_STG_DATASET_NM, MS_STG_TBL_NM, \
        MS_RAW_FILE_BUCKET_NM, MS_RAW_FILE_BUCKET_DIR, MS_RAW_FILE_TYPE, \
        MS_TFD_FILE_BUCKET_NM, MS_TFD_FILE_BUCKET_DIR, MS_TFD_FILE_TYPE
    
    config, env_vars = load_config()
    
    # Set variables based on config and environment variables
    #GC_PROJECT_ID = env_vars["PROJECT_ID"]
    _, GC_PROJECT_ID = ga_default()
    GC_ENV = env_vars["ENVIRONMENT"]
    
    ms_cfg = config["MARKET_STACK_METADATA"]
    ms_cat_cfg_nm = f"MARKET_STACK_{data_cat.upper()}_METADATA"
    if ms_cat_cfg_nm in config: ms_cat_cfg = config[ms_cat_cfg_nm] 
    else: raise Exception(f"{data_cat} not a supported data category for Market Stack API wihin config.ini file")
    MS_CAT = ms_cat_cfg["name"]
    MS_SYMBOLS_LST = [symbol.strip() for symbol in ms_cfg["symbols"].split(",")]
    MS_DATA_CTGYS_LST = [data_cat.strip() for data_cat in ms_cfg["data_ctgys"].split(",")]
    MS_BASE_URL = ms_cfg["base_url"]
    MS_CAT_URL = f"{MS_BASE_URL}{'' if MS_BASE_URL.endswith('/') else '/'}{ms_cat_cfg.lower()}"
    
    MS_V2_API_KEY = get_secret("MS_V2_API_KEY")
    
    ## Extract source data
    MS_RAW_FILE_TYPE = ms_cat_cfg["raw_file_type"]
    MS_RAW_FILE_BUCKET_NM = f"{ms_cat_cfg['raw_file_bucket_base']}-{GC_ENV}"
    MS_RAW_FILE_BUCKET_DIR = ms_cat_cfg["raw_file_bucket_dir"]
    
    ## Transformed data
    MS_TFD_FILE_TYPE = ms_cat_cfg["tfd_file_type"]
    MS_TFD_FILE_BUCKET_NM = f"{ms_cat_cfg['tfd_file_bucket_base']}-{GC_ENV}"
    MS_TFD_FILE_BUCKET_DIR = ms_cat_cfg["tfd_file_bucket_dir"]
    
    ## Cleaned data location
    MS_TGT_DATASET_NM = f"{ms_cfg['bq_target_dataset_base']}-{GC_ENV}"
    MS_STG_DATASET_NM = f"{ms_cfg['bq_staging_dataset_base']}-{GC_ENV}"
    MS_TGT_TBL_NM = MS_CAT
    MS_STG_TBL_NM = f"{MS_TGT_TBL_NM}_stg"
