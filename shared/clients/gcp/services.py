# GCP services client wrapper to interact with various GCP services like Secret Manager, Cloud Storage, and Dataproc

# Builtin imports
from io import BytesIO
import json
from json import dumps as json_dumps, loads as json_loads, JSONDecodeError
from natsort import natsort_keygen, ns
import logging
import pandas as pd
from typing import Sequence, Hashable

# Shared imports
from google.api_core.exceptions import NotFound, Forbidden
from google.cloud import storage as gc_storage
from shared.clients.gcp.naming_conv import GCS_PREFIX, GCS_FULL_FILE_PATH_V1

def check_blob_exists(bucket_nm, blob_nm):
    storage_client = gc_storage.Client()
    bucket = storage_client.bucket(bucket_nm)
    blob = bucket.blob(blob_nm)

    if blob.exists():
        print(f"Blob exists: {GCS_PREFIX}{GCS_FULL_FILE_PATH_V1(bucket_nm, blob_nm)}")
        return True
    else:
        print(f"Blob '{blob_nm}' does not exist in bucket '{bucket_nm}'.")
        return False

def read_json_gcs(bucket_nm: str, blob_nm: str):
    try:
        client = gc_storage.Client()
        bucket = client.bucket(bucket_nm)
        blob = bucket.blob(blob_nm)

        content = blob.download_as_text()
        return json_loads(content)

    except NotFound as e:
        logging.error(f"Eile at {GCS_PREFIX}{GCS_FULL_FILE_PATH_V1(bucket_nm, blob_nm)} not found: {e}")
    except Forbidden as e:
        logging.error(f"Eile at {GCS_PREFIX}{GCS_FULL_FILE_PATH_V1(bucket_nm, blob_nm)} is lacking proper IAM roles by user/SA: {e}")
    except JSONDecodeError as e:
        logging.error(f"Eile at {GCS_PREFIX}{GCS_FULL_FILE_PATH_V1(bucket_nm, blob_nm)} could not be decoded as json: {e}")
    except Exception as e:
        logging.error(f"Running into issues w/ {GCS_PREFIX}{GCS_FULL_FILE_PATH_V1(bucket_nm, blob_nm)}: {e}")
    return None


def write_json_gcs(data, bucket_nm: str, blob_nm: str):
    file_type = "json"
    try:
        client = gc_storage.Client()
        bucket = client.bucket(bucket_nm)
        blob = bucket.blob(blob_nm)

        blob.upload_from_string(
            json_dumps(data),
            content_type=f"application/{file_type}"
        )

        return f"{GCS_PREFIX}{GCS_FULL_FILE_PATH_V1(bucket_nm, blob_nm)}"

    except Forbidden as e:
        logging.error(f"Eile at {GCS_PREFIX}{GCS_FULL_FILE_PATH_V1(bucket_nm, blob_nm)} is lacking proper IAM roles by user/SA: {e}")
    except Exception as e:
        logging.error(f"Running into issues w/ {GCS_PREFIX}{GCS_FULL_FILE_PATH_V1(bucket_nm, blob_nm)}: {e}")
    return None


def read_parquet_gcs(bucket_nm: str, blob_nm: str):
    read_parq_eng_type = "pyarrow"
    try:
        client = gc_storage.Client()
        bucket = client.bucket(bucket_nm)
        blob = bucket.blob(blob_nm)

        parquet_bytes = blob.download_as_bytes()
        buffer = BytesIO(parquet_bytes)

        return pd.read_parquet(buffer, engine=read_parq_eng_type)

    except NotFound as e:
        logging.error(f"Eile at {GCS_PREFIX}{GCS_FULL_FILE_PATH_V1(bucket_nm, blob_nm)} not found: {e}")
    except Forbidden as e:
        logging.error(f"Eile at {GCS_PREFIX}{GCS_FULL_FILE_PATH_V1(bucket_nm, blob_nm)} is lacking proper IAM roles by user/SA: {e}")
    except Exception as e:
        logging.error(f"Running into issues w/ {GCS_PREFIX}{GCS_FULL_FILE_PATH_V1(bucket_nm, blob_nm)}: {e}")
    return None


def write_parquet_gcs(df: pd.DataFrame, bucket_nm: str, blob_nm: str, partition_cols: Sequence[Hashable] | None = None):
    save_type = "octet-stream"
    try:
        client = gc_storage.Client()
        bucket = client.bucket(bucket_nm)
        blob = bucket.blob(blob_nm)
        
        buffer = BytesIO()
        df.to_parquet(buffer, index=False, partition_cols=partition_cols)
        buffer.seek(0)

        blob.upload_from_file(buffer, content_type=f"application/{save_type}")
        
        return f"{GCS_PREFIX}{GCS_FULL_FILE_PATH_V1(bucket_nm, blob_nm)}"

    except Forbidden as e:
        logging.error(f"Eile at {GCS_PREFIX}{GCS_FULL_FILE_PATH_V1(bucket_nm, blob_nm)} is lacking proper IAM roles by user/SA: {e}")
    except Exception as e:
        logging.error(f"Running into issues w/ {GCS_PREFIX}{GCS_FULL_FILE_PATH_V1(bucket_nm, blob_nm)}: {e}")
    return None

def convert_dict_pandas_df(data: dict):
    return pd.DataFrame(data)