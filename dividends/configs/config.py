
# Configuration file for data extractionm transformation, loading, and storage parameters for Market Stack data. 
# This file defines constants for API keys, URLs, file formats, and GCS bucket details used across the ETL pipeline.

# imports
import os
import configparser
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_ENV_VAR_NAME = "APP_ENV"
DEFAULT_ENV_VAR_VAL = "dev"

env = os.getenv(DEFAULT_ENV_VAR_NAME, DEFAULT_ENV_VAR_VAL)
config = configparser.ConfigParser()
config.read(BASE_DIR / "config.ini") # Load base first
config.read(BASE_DIR / f"config.{env}.ini") # Then environment override

ms_cfg = config["MARKET_STACK_METADATA"]
ms_div_cfg = config["MARKET_STACK_DIVIDENDS_METADATA"]

MS_SYMBOLS = ms_cfg["symbols"]
MS_BASE_URL = ms_cfg["base_url"]

MS_CAT = ms_div_cfg["name"]
MS_DIV_URL = f"{MS_BASE_URL}{MS_CAT}"

## Extract source data
MS_DIV_RAW_FILE_TYPE = ms_div_cfg["raw_file_type"]
MS_DIV_RAW_FILE_BUCKET_NM = ms_div_cfg["raw_file_bucket"]
MS_DIV_RAW_FILE_BUCKET_SUBDIR = ms_div_cfg["raw_file_bucket_subdir"]

## Transformed data
MS_DIV_TFD_FILE_TYPE = ms_div_cfg["tfd_file_type"]
MS_DIV_TFD_FILE_BUCKET_NM = ms_div_cfg["tfd_file_bucket"]
MS_DIV_TFD_FILE_BUCKET_SUBDIR = ms_div_cfg["tfd_file_bucket_subdir"]

## Cleaned data
MS_DIV_CLN_FILE_TYPE = ms_div_cfg["cln_file_type"]
MS_DIV_CLN_FILE_BUCKET_NM = ms_div_cfg["cln_file_bucket"]
MS_DIV_CLN_FILE_BUCKET_SUBDIR = ms_div_cfg["cln_file_bucket_subdir"]

