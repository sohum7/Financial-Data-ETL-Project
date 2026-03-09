# GCP services client wrapper to interact with various GCP services like Secret Manager, Cloud Storage, and Dataproc

# Builtin imports
from json import dumps as json_dumps
from os import environ as os_environ

# Shared imports
from google.cloud import secretmanager as gc_secretmanager
from google.cloud import storage as gc_storage
from pyspark.sql import SparkSession
from shared.clients.gcp_gcs_naming import MS_FILE_NM, GCS_BUCKET_PATH,GCS_BLOB_PATH, DF_SAVE_PATH


def get_secret(secret_name):
    project_id = os_environ.get("GOOGLE_CLOUD_PROJECT")
    client = gc_secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

def write_json_to_gcs(data, bucket_nm, dir_path, file_nm, batch_dt, start_dt, end_dt) -> None:
    file_type = "json"
    
    blob_nm = GCS_BLOB_PATH(dir_path, file_nm, file_type)
    
    storage_client = gc_storage.Client()
    bucket_obj = storage_client.bucket(bucket_nm)
    blob_obj = bucket_obj.blob(blob_nm)

    # Upload the JSON data to GCS
    blob_obj.upload_from_string(json_dumps(data, indent=4), content_type=f"application/{file_type}")

# Read JSON data from GCS using Spark
"""
def read_json_from_gcs(data_cat, bucket_nm, dir_path, file_nm, batch_dt, with_spark=False, **kwargs):
    #TODO FIX THE FILENAME PART
    if with_spark:
        with SparkSession.builder.appName(f"read_json_from_gcs_{data_cat.upper()}").getOrCreate() as spark:
            return spark.read.json(f"........").cache() # read the JSON data from GCS using Spark and return as Spark DataFrame. We will cache the DataFrame since we will be performing multiple transformations on it in the transform step, so caching will help improve performance by avoiding repeated reads from GCS.
    
    # else use native GCS client to read the JSON data from GCS and return as dict
    storage_client = gc_storage.Client()
    bucket_obj = storage_client.bucket(bucket_nm)
    return bucket_obj.blob(GCS_BLOB_PATH(batch_dt, dir_nm, file_nm))
"""


# Write the transformed data back to GCS in delta lake format (parquet), partitioned by market_dt and clustered by symbol
def write_df_to_gcs(df, bucket_nm, dir_path, partition_col, cluster_col, file_type, save_mode):
    
    file_path = f"{DF_SAVE_PATH(bucket_nm, dir_path)}"
    df.write \
        .format(file_type) \
        .mode(save_mode) \
        .save(file_path)
        
    """
    df.sortWithinPartitions(cluster_col) \
        .write \
        .format(file_type) \
        .partitionBy(partition_col) \
        .mode(save_mode) \
        .save(file_path)
    """
    return file_path
