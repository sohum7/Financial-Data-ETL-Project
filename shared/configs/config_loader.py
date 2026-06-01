# Configuration file for data extractionm transformation, loading, and storage parameters for Market Stack data. 
# This file defines constants for API keys, URLs, file formats, and GCS bucket details used across the ETL pipeline.

# Built-in imports
from configparser import ConfigParser
from dataclasses import dataclass
from dotenv import load_dotenv
from os import getenv as os_getenv
from pathlib import Path

# Shared imports
from shared.clients.gcp.bq import TableConfig, PartitionConfig
from shared.clients.gcp.gcs import MS_FILE_NM, FileConfig
from shared.clients.ms.api import APIConfig

# Google API imports
from google.auth import default as ga_default
from google.cloud import secretmanager as gc_secretmanager


# ETL settings encapsulation
@dataclass
class ETLSettings:
    gc_project_id: str
    gc_env: str
    ms_api_obj: APIConfig
    ms_raw_gcs_path: FileConfig
    ms_tfd_gcs_path: FileConfig
    ms_tgt_bq_metadata: TableConfig
    ms_stg_bq_metadata: TableConfig

# TODO: May need to move this to gcp shared folder
# Obtain secret API key from GCP secret manager
def get_secret(secret_name) -> str:
    _, project_id = ga_default()
    decode_type = "UTF-8"
    
    # Get the secret key
    client = gc_secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    
    return response.payload.data.decode(decode_type)

# Load configuration and .env file
def load_config() -> tuple[ConfigParser, dict[str, str]]:
    # Load config.ini file
    BASE_DIR = Path(__file__).resolve().parent
    config = ConfigParser()
    config.read(BASE_DIR / "config.ini")
    
    # load .env variable(s)
    load_dotenv()
    env_vars = { "ENV": os_getenv("ENVIRONMENT", "") }
    _, project_id = ga_default()
    env_vars["PROJECT_ID"] = project_id or ""
    
    # Ensure certain fields are not missing
    if not env_vars["PROJECT_ID"]:
        raise ValueError("project_id not obtained from ADC'\n and env variable PROJECT_ID missing")
    
    if not env_vars["ENV"]:  
        raise ValueError("missing required environment variable: ENVIRONMENT")
    
    return config, env_vars

# Load settings necessary for pipeline to run
def load_settings(data_type, start_dt, end_dt, batch_dt) -> ETLSettings:
    # Load configuration and environment variable
    config, env_vars = load_config()
    GC_ENV = env_vars['ENV']
    
    # Ensure data category is validated
    ms_cat = data_type.upper()
    ms_cfg = config["MARKET_STACK_METADATA"]
    ms_cat_cfg_nm = f"MARKET_STACK_{ms_cat}_METADATA" # Most exist in config.ini other wise considered not a valid category
    if ms_cat_cfg_nm not in config: raise Exception(f"{data_type.upper()} not a supported data category for Market Stack API wihin config.ini file")
    ms_cat_cfg = config[ms_cat_cfg_nm]
    
    # Set MS API metadata variables from config
    MS_CAT = ms_cat_cfg["name"]
    MS_SYMBOLS_LST = [symbol.strip() for symbol in ms_cfg["symbols"].split(",")]
    MS_BASE_URL_V2 = ms_cfg["base_url"]
    MS_V2_API_KEY = get_secret("MARKET_STACK_V2_API_KEY")
    
    ## Extraction GCS variables from config
    MS_RAW_FILE_BUCKET_DIR = ms_cat_cfg["raw_file_bucket_dir"].strip('/')
    MS_RAW_FILE_BUCKET_NM = f"{ms_cat_cfg['raw_file_bucket_base']}-{GC_ENV}"
    MS_RAW_FILE_TYPE = ms_cat_cfg["raw_file_type"]
    
    ## Transformation GCS variables from config
    MS_TFD_FILE_BUCKET_DIR = ms_cat_cfg["tfd_file_bucket_dir"].strip('/')
    MS_TFD_FILE_BUCKET_NM = f"{ms_cat_cfg['tfd_file_bucket_base']}-{GC_ENV}"
    MS_TFD_FILE_TYPE = ms_cat_cfg["tfd_file_type"]
    
    ## Loading BQ variables from config
    MS_TGT_DATASET_NM = f"{ms_cfg['bq_target_dataset_base']}_{GC_ENV}"
    MS_TGT_TBL_NM = MS_CAT
    MS_TGT_PART_COL = f"{ms_cfg['bq_partition_column']}"
    MS_TGT_PART_COL_GRAN = f"{ms_cfg['bq_partition_column_granularity']}"
    MS_TGT_CLUST_COLS = ms_cfg['bq_cluster_columns'].split(",")
    MS_STG_DATASET_NM = f"{ms_cfg['bq_staging_dataset_base']}_{GC_ENV}"
    MS_STG_TBL_NM = f"{MS_TGT_TBL_NM}_stg"
    
    # Define parameters for the ETL process    
    ms_api_obj: APIConfig = APIConfig(MS_CAT, MS_BASE_URL_V2, MS_SYMBOLS_LST, MS_V2_API_KEY, start_dt, end_dt, batch_dt)
    
    # Define the weekly subdirectory path and file name for GCS storage of raw and transformed data based on the data category, start date, end date, batch date, 
    # and unique hash value derived from the APIConfig attributes for better organization of files in GCS
    sub_dir: str = f"{ms_api_obj.data_type}/{batch_dt}/start={ms_api_obj.start_dt}_end={ms_api_obj.end_dt}" # .strip('/')
    file_nm: str = MS_FILE_NM(ms_api_obj.data_type, ms_api_obj.start_dt, ms_api_obj.end_dt, ms_api_obj.hash_val)
    
    return ETLSettings( env_vars["PROJECT_ID"],
                        env_vars["ENV"],
                        ms_api_obj,
                        FileConfig(
                            MS_RAW_FILE_BUCKET_NM, 
                            f"{MS_RAW_FILE_BUCKET_DIR}/{sub_dir}", 
                            file_nm, 
                            MS_RAW_FILE_TYPE),
                        FileConfig(
                            MS_TFD_FILE_BUCKET_NM, 
                            f"{MS_TFD_FILE_BUCKET_DIR}/{sub_dir}", 
                            file_nm, 
                            MS_TFD_FILE_TYPE),
                        TableConfig(
                            MS_TGT_DATASET_NM, 
                            MS_TGT_TBL_NM,
                            PartitionConfig(
                                MS_TGT_PART_COL,
                                MS_TGT_PART_COL_GRAN),
                            MS_TGT_CLUST_COLS),
                        TableConfig(
                            MS_STG_DATASET_NM, 
                            MS_STG_TBL_NM,
                            None,
                            None)
    )
