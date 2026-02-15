# Configuration file for dividends data extraction and transformation


MS_FILE_FORMAT = lambda DATA_TYPE, START_DT, END_DT, FILE_TYPE: f"{DATA_TYPE}_{START_DT.strftime('%Y%m%d')}_{END_DT.strftime('%Y%m%d')}.{FILE_TYPE}"

## Extract source data
MS_DIVIDENDS = "DIVIDENDS_DATA"

MS_DIVIDENDS_API_KEY = "25d2f25478972551751d275bd4048f10"
MS_DIVIDENDS_URL = "https://api.marketstack.com/v2/dividends"

MS_DIVIDENDS_RAW_FILE_TYPE = "json"
MS_DIVIDENDS_RAW_FILE_BUCKET = "market-stack-data-dev"
MS_DIVIDENDS_RAW_FILE_BUCKET_SUBDIR = "bronze/dividends/"

## Transformed data
MS_DIVIDENDS_TFD_FILE_TYPE = "delta"
MS_DIVIDENDS_TFD_FILE_BUCKET = "market-stack-data-dev"
MS_DIVIDENDS_TFD_FILE_BUCKET_SUBDIR = "silver/dividends/"

## Cleaned data
MS_DIVIDENDS_CLN_FILE_TYPE = "parquet"
MS_DIVIDENDS_CLN_FILE_BUCKET = "market-stack-data-dev"
MS_DIVIDENDS_CLN_FILE_BUCKET_SUBDIR = "gold/dividends/"
