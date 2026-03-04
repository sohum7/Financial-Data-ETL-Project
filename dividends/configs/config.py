
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

MS_DIVIDENDS = config["DIVIDENDS_METADATA"]["name"]
MS_DIVIDENDS_BASE_URL = config["DIVIDENDS_METADATA"]["url"]
MS_DIVIDENDS_DIVIDENDS_URL = MS_DIVIDENDS_BASE_URL + MS_DIVIDENDS
MS_DIVIDENDS_SYMBOLS = config["DIVIDENDS_METADATA"]["symbols"]

## Extract source data
MS_DIVIDENDS_RAW_FILE_TYPE = config["DIVIDENDS_METADATA"]["raw_file_type"]
MS_DIVIDENDS_RAW_FILE_BUCKET = config["DIVIDENDS_METADATA"]["raw_file_bucket"]
MS_DIVIDENDS_RAW_FILE_BUCKET_SUBDIR = config["DIVIDENDS_METADATA"]["raw_file_bucket_subdir"]

## Transformed data
MS_DIVIDENDS_TFD_FILE_TYPE = "delta"
MS_DIVIDENDS_TFD_FILE_BUCKET = "market-stack-data-dev"
MS_DIVIDENDS_TFD_FILE_BUCKET_SUBDIR = "silver/dividends/"

## Cleaned data
MS_DIVIDENDS_CLN_FILE_TYPE = "parquet"
MS_DIVIDENDS_CLN_FILE_BUCKET = "market-stack-data-dev"
MS_DIVIDENDS_CLN_FILE_BUCKET_SUBDIR = "gold/dividends/"

