from json import dumps as json_dumps
from pyspark.sql import SparkSession
#from datetime import datetime
from os import environ as os_environ
from google.cloud import storage
from google.cloud import secretmanager


FILE_NM_DT_FORMAT = "%Y%m%d"
# raw file format in gcs bucket for extracted data. This is used to construct the file name for the raw data stored in GCS. The file name is based on the data category, start date, end date, and file type (e.g., json).
# Example: {file_status_type}_dividends_20240101_20240107_20240107
MS_FILE_NM_WO_EXT = lambda DATA_CAT, BATCH_DT, START_DT, END_DT: f"{DATA_CAT.lower()}_{START_DT.strftime(FILE_NM_DT_FORMAT)}_{END_DT.strftime(FILE_NM_DT_FORMAT)}_{BATCH_DT.strftime(FILE_NM_DT_FORMAT)}"
MS_FILE_NM_W_EXT = lambda DATA_CAT, BATCH_DT, START_DT, END_DT, FILE_TYPE: f"{MS_FILE_NM_WO_EXT(DATA_CAT, BATCH_DT, START_DT, END_DT)}.{FILE_TYPE.lower()}"

GCS_PREFIX = "gs://"
GCS_DIR_PATH = lambda BATCH_DT, BUCKET_NM, DIR_NM: f"{GCS_PREFIX}{BUCKET_NM}/{DIR_NM}/batch_date={BATCH_DT.strftime(FILE_NM_DT_FORMAT)}/"
GCS_FILE_PATH = lambda BATCH_DT, BUCKET_NM, DIR_NM, FILE_NM: f"{GCS_DIR_PATH(BATCH_DT, BUCKET_NM, DIR_NM)}{FILE_NM}"

def get_secret(secret_name):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{os_environ['GCP_PROJECT']}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

def write_json_to_gcs(data_cat, data, bucket_nm, dir_nm, batch_dt, start_dt, end_dt) -> None:
    file_nm = MS_FILE_NM_W_EXT(data_cat, batch_dt, start_dt, end_dt, "json")
    blob_nm = GCS_FILE_PATH(batch_dt, bucket_nm, dir_nm, file_nm)
    
    storage_client = storage.Client()
    bucket_obj = storage_client.bucket(bucket_nm)
    blob_obj = bucket_obj.blob(blob_nm)

    # Upload the JSON data to GCS
    blob_obj.upload_from_string(json_dumps(data, indent=4), content_type="application/json")

# Read JSON data from GCS using Spark
def read_json_from_gcs(data_cat, bucket_nm, dir_nm, file_nm, batch_dt, with_spark=False, **kwargs):
    if with_spark:
        with SparkSession.builder.appName(f"read_json_from_gcs_{data_cat.upper()}").getOrCreate() as spark:
            return spark.read.json(GCS_FILE_PATH(batch_dt, bucket_nm, dir_nm, file_nm)).cache() # read the JSON data from GCS using Spark and return as Spark DataFrame. We will cache the DataFrame since we will be performing multiple transformations on it in the transform step, so caching will help improve performance by avoiding repeated reads from GCS.
        
    # else use native GCS client to read the JSON data from GCS and return as dict
    storage_client = storage.Client()
    bucket_obj = storage_client.bucket(bucket_nm)
    return bucket_obj.blob(GCS_FILE_PATH(batch_dt, bucket_nm, dir_nm, file_nm))


# Write the transformed data back to GCS in delta lake format (parquet), partitioned by market_dt and clustered by symbol
def write_dividends_df_to_gcs(df, data_cat, bucket_nm, bucket_dir_nm, partition_col, cluster_cols, file_type, save_mode, batch_dt, start_dt, end_dt):
    file_nm = MS_FILE_NM_WO_EXT(data_cat, batch_dt, start_dt, end_dt)
    file_path = GCS_FILE_PATH(batch_dt, bucket_nm, bucket_dir_nm, file_nm)
    df.write \
        .format(file_type) \
        .partitionBy(partition_col) \
        .sortBy(*cluster_cols) \
        .mode(save_mode) \
        .save(file_path)
    return file_path
