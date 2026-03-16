
# Configuration file for data extractionm transformation, loading, and storage parameters for Market Stack data. 
# This file defines constants for API keys, URLs, file formats, and GCS bucket details used across the ETL pipeline.

# Built-in imports
from configparser import ConfigParser
from dotenv import load_dotenv
from os import getenv as os_getenv
from pathlib import Path

# Google API imports
from google.auth import default as ga_default
from google.auth.exceptions import DefaultCredentialsError as ga_DefaultCredentialsError
from google.cloud import secretmanager as gc_secretmanager


def get_secret(secret_name):
    decode_type = "UTF-8"
    #project_id = os_getenv("PROJECT_ID")
    global project_id
    _, project_id = ga_default()
    client = gc_secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return project_id, response.payload.data.decode(decode_type)

def load_config() -> tuple[ ConfigParser, dict[ str, str]]:
    BASE_DIR = Path(__file__).resolve().parent
    
    load_dotenv()
    
    config = ConfigParser()
    config.read(BASE_DIR / "config.ini") # Load base first
    
    env_vars = {
        "ENVIRONMENT": os_getenv("ENVIRONMENT", ""),
        "PROJECT_ID": os_getenv("PROJECT_ID", ""),
        "API_KEY": ""
    }
    
    project_id, env_vars["API_KEY"] = get_secret("MARKET_STACK_V2_API_KEY")
    if not env_vars["API_KEY"]:  
        raise ValueError("Missing Application Default Credentials. Please set up ADC.")
    if project_id is None     and not env_vars["PROJECT_ID"]: raise ValueError("project_id not obtained from ADC'\n and env variable PROJECT_ID missing")
    if project_id is not None and not env_vars["PROJECT_ID"]: env_vars["PROJECT_ID"] = project_id
    if project_id is None     and     env_vars["PROJECT_ID"]: project_id = env_vars["PROJECT_ID"]
    if project_id is not None and     env_vars["PROJECT_ID"] and project_id != env_vars["PROJECT_ID"]: 
        raise ValueError("project_id from ADC does not match with the env variable PROJECT_ID")
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
    GC_PROJECT_ID = env_vars["PROJECT_ID"]
    GC_ENV = env_vars["ENVIRONMENT"]
    MS_V2_API_KEY = env_vars["API_KEY"]
    
    ms_cfg = config["MARKET_STACK_METADATA"]
    ms_cat_cfg_nm = f"MARKET_STACK_{data_cat.upper()}_METADATA"
    if ms_cat_cfg_nm in config: ms_cat_cfg = config[ms_cat_cfg_nm]
    else: raise Exception(f"{data_cat} not a supported data category for Market Stack API wihin config.ini file")
    MS_CAT = ms_cat_cfg["name"]
    MS_SYMBOLS_LST = [symbol.strip() for symbol in ms_cfg["symbols"].split(",")]
    MS_DATA_CTGYS_LST = [data_cat.strip() for data_cat in ms_cfg["data_ctgys"].split(",")]
    MS_BASE_URL = ms_cfg["base_url"]
    MS_CAT_URL = f"{MS_BASE_URL}{'' if MS_BASE_URL.endswith('/') else '/'}{MS_CAT.lower()}"
    
    ## Extract source data
    MS_RAW_FILE_TYPE = ms_cat_cfg["raw_file_type"]
    MS_RAW_FILE_BUCKET_NM = f"{ms_cat_cfg['raw_file_bucket_base']}-{GC_ENV}"
    MS_RAW_FILE_BUCKET_DIR = ms_cat_cfg["raw_file_bucket_dir"]
    
    ## Transformed data
    MS_TFD_FILE_TYPE = ms_cat_cfg["tfd_file_type"]
    MS_TFD_FILE_BUCKET_NM = f"{ms_cat_cfg['tfd_file_bucket_base']}-{GC_ENV}"
    MS_TFD_FILE_BUCKET_DIR = ms_cat_cfg["tfd_file_bucket_dir"]
    
    ## Cleaned data location
    MS_TGT_DATASET_NM = f"{ms_cfg['bq_target_dataset_base']}_{GC_ENV}"
    MS_STG_DATASET_NM = f"{ms_cfg['bq_staging_dataset_base']}_{GC_ENV}"
    MS_TGT_TBL_NM = MS_CAT
    MS_STG_TBL_NM = f"{MS_TGT_TBL_NM}_stg"
