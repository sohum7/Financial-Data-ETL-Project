# GCP services client wrapper to interact with various GCP services like Secret Manager, Cloud Storage, and Dataproc

# Built-in imports
from io import BytesIO
from json import dumps as json_dumps, loads as json_loads, JSONDecodeError
from natsort import natsort_keygen, ns
import logging
import pandas as pd
from typing import Sequence, Hashable

# Shared imports
from shared.clients.gcp.naming_conv import GCSPathLib

# Google API imports
from google.api_core.exceptions import NotFound, Forbidden
from google.cloud import storage as gc_storage


# Get ....
def get_blob_resources(bucket_nm, blob_nm):
    storage_client_obj = gc_storage.Client()
    bucket_obj = storage_client_obj.bucket(bucket_nm)
    blob_obj = bucket_obj.blob(blob_nm)
    return storage_client_obj, bucket_obj, blob_obj

# Check if a blob exists
def check_blob_exists(bucket_nm, blob_nm):
    _, _, blob_obj = get_blob_resources(bucket_nm, blob_nm)
    blob_path = GCSPathLib.blob_path_static(bucket_nm, blob_nm)
    
    if blob_obj.exists():
        print(f"Blob exists: {blob_path}")
        return True
    else:
        print(f"Blob does not exist: {blob_path}")
        return False

# Read json from gcs path
def read_json_gcs(bucket_nm: str, blob_nm: str):
    blob_path = GCSPathLib.blob_path_static(bucket_nm, blob_nm)
    try:
        _, _, blob_obj = get_blob_resources(bucket_nm, blob_nm)
        
        content = blob_obj.download_as_text()
        return json_loads(content)
    
    except NotFound as e:
        logging.error(f"Eile at {blob_path} not found: {e}")
    except Forbidden as e:
        logging.error(f"Eile at {blob_path} is lacking proper IAM roles by user/SA: {e}")
    except JSONDecodeError as e:
        logging.error(f"Eile at {blob_path} could not be decoded as json: {e}")
    except Exception as e:
        logging.error(f"Running into issues w/ {blob_path}: {e}")
    return None

# Write json from gcs path
def write_json_gcs(data, bucket_nm: str, blob_nm: str):
    blob_path = GCSPathLib.blob_path_static(bucket_nm, blob_nm)
    file_type = "json"
    try:
        _, _, blob_obj = get_blob_resources(bucket_nm, blob_nm)
        
        blob_obj.upload_from_string(
            json_dumps(data),
            content_type=f"application/{file_type}"
        )
        
        return f"{blob_path}"
    except Forbidden as e:
        logging.error(f"Eile at {blob_path} is lacking proper IAM roles by user/SA: {e}")
    except Exception as e:
        logging.error(f"Running into issues w/ {blob_path}: {e}")
    return None

# Read parquet from gcs path
def read_parquet_gcs(bucket_nm: str, blob_nm: str):
    blob_path = GCSPathLib.blob_path_static(bucket_nm, blob_nm)
    read_parquet_eng_type = "pyarrow"
    try:
        _, _, blob_obj = get_blob_resources(bucket_nm, blob_nm)
        
        parquet_bytes = blob_obj.download_as_bytes()
        buffer = BytesIO(parquet_bytes)
        
        return pd.read_parquet(buffer, engine=read_parquet_eng_type)
    except NotFound as e:
        logging.error(f"Eile at {blob_path} not found: {e}")
    except Forbidden as e:
        logging.error(f"Eile at {blob_path} is lacking proper IAM roles by user/SA: {e}")
    except Exception as e:
        logging.error(f"Running into issues w/ {blob_path}: {e}")
    return None

# Write parquet from gcs path
def write_parquet_gcs(df: pd.DataFrame, bucket_nm: str, blob_nm: str, partition_cols: Sequence[Hashable] | None = None):
    blob_path = GCSPathLib.blob_path_static(bucket_nm, blob_nm)
    save_type = "octet-stream"
    try:
        _, _, blob_obj = get_blob_resources(bucket_nm, blob_nm)
        
        buffer = BytesIO()
        df.to_parquet(buffer, index=False, partition_cols=partition_cols)
        buffer.seek(0)
        
        blob_obj.upload_from_file(buffer, content_type=f"application/{save_type}")
        
        return f"{blob_path}"
    
    except Forbidden as e:
        logging.error(f"Eile at {blob_path} is lacking proper IAM roles by user/SA: {e}")
    except Exception as e:
        logging.error(f"Running into issues w/ {blob_path}: {e}")
    return None

# Convert python dictionary to a pandas DataFrame
def convert_dict_pandas_df(data: dict):
    return pd.json_normalize(data)
    #return pd.DataFrame(data)