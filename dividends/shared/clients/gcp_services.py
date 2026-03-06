from json import dumps as json_dumps
from pyspark.sql import SparkSession
#from datetime import datetime
from os import environ as os_environ
from google.cloud import storage as gc_storage
from google.cloud import secretmanager as gc_secretmanager
from gcs_naming import MS_FILE_NM_W_EXT, GCS_BLOB_PATH, GCS_BLOB_PATH_PREFIX, GCS_BUCKET_PATH

def get_secret(secret_name):
    project_id = os_environ.get("GOOGLE_CLOUD_PROJECT")
    client = gc_secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

def write_json_to_gcs(data_cat, data, bucket_nm, dir_nm, batch_dt, start_dt, end_dt) -> None:
    file_type = "json"
    file_nm = MS_FILE_NM_W_EXT(data_cat, start_dt, end_dt, file_type)
    blob_nm = GCS_BLOB_PATH(batch_dt, dir_nm, file_nm)
    
    storage_client = gc_storage.Client()
    bucket_obj = storage_client.bucket(bucket_nm)
    blob_obj = bucket_obj.blob(blob_nm)

    # Upload the JSON data to GCS
    blob_obj.upload_from_string(json_dumps(data, indent=4), content_type=f"application/{file_type}")

# Read JSON data from GCS using Spark
def read_json_from_gcs(data_cat, bucket_nm, dir_nm, file_nm, batch_dt, with_spark=False, **kwargs):
    #TODO FIX THE FILENAME PART
    GCS_BLOB_PATH_PREFIX(batch_dt, dir_nm) # this is the prefix for the blob path, which is the directory structure in GCS where the file is stored. we will use this prefix to construct the full blob path to read the file from GCS. the full blob path will be {dir_nm}/batch_date={batch_dt}/{file_nm}.json
    if with_spark:
        with SparkSession.builder.appName(f"read_json_from_gcs_{data_cat.upper()}").getOrCreate() as spark:
            return spark.read.json(f"{GCS_BUCKET_PATH(bucket_nm)}/{GCS_BLOB_PATH_PREFIX(batch_dt, dir_nm)}/*").cache() # read the JSON data from GCS using Spark and return as Spark DataFrame. We will cache the DataFrame since we will be performing multiple transformations on it in the transform step, so caching will help improve performance by avoiding repeated reads from GCS.
        
    # else use native GCS client to read the JSON data from GCS and return as dict
    storage_client = gc_storage.Client()
    bucket_obj = storage_client.bucket(bucket_nm)
    return bucket_obj.blob(GCS_BLOB_PATH(batch_dt, dir_nm, file_nm))


# Write the transformed data back to GCS in delta lake format (parquet), partitioned by market_dt and clustered by symbol
def write_dividends_df_to_gcs(df, data_cat, bucket_nm, bucket_dir_nm, partition_col, cluster_cols, file_type, save_mode, batch_dt, start_dt, end_dt):
    file_nm = MS_FILE_NM(data_cat, start_dt, end_dt)
    file_path = GCS_BLOB_PATH(batch_dt, bucket_dir_nm, file_nm)
    df.write \
        .format(file_type) \
        .partitionBy(partition_col) \
        .sortBy(*cluster_cols) \
        .mode(save_mode) \
        .save(file_path)
    return file_path
