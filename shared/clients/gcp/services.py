# GCP services client wrapper to interact with various GCP services like Secret Manager, Cloud Storage, and Dataproc

# Built-in imports
from io import BytesIO
from json import dumps as json_dumps, loads as json_loads
import logging
import pandas as pd
from typing import Sequence, Hashable

# Shared imports
from shared.clients.gcp.naming_conv import GCSPathLib
from shared.clients.gcp.logging import GCPLogger

# Google API imports
from google.cloud import bigquery as gc_bq
from google.cloud import storage as gc_storage

# Get ....
def create_dataset(dataset_id: str, location: str, logger: logging.Logger | GCPLogger) -> tuple[gc_bq.Client, gc_bq.Dataset] :
    """_summary_

    Args:
        dataset_id (str): _description_
        location (str): _description_
        logger (logging.Logger | GCPLogger): _description_

    Returns:
        tuple[gc_bq.Client, gc_bq.Dataset]: _description_

    Raises:
        google.auth.exceptions.DefaultCredentialsError: If credentials is not specified and the library fails to acquire default credentials.
        google.api_core.exceptions.Conflict: If the dataset already exists.
    """
    
    bq_client = gc_bq.Client()  # Uses default project
    full_dataset_id = f"{bq_client.project}.{dataset_id}"
    
    bq_dataset = gc_bq.Dataset(full_dataset_id)
    bq_dataset.location = location
    
    # Create dataset if it does not exist (exists_ok=True avoids error if it exists)
    logger.info(f"Creating dataset if it does not exist: {full_dataset_id} in location: {location}")
    bq_dataset = bq_client.create_dataset(bq_dataset, exists_ok=True)
    
    logger.info(f"Dataset created or already exists: {full_dataset_id} in location: {location}")
    return bq_client, bq_dataset

# Get ....
def get_blob_resources(bucket_nm: str, blob_nm: str) -> tuple[gc_storage.Client, gc_storage.Bucket, gc_storage.Blob]:
    """_summary_

    Args:
        bucket_nm (str): _description_
        blob_nm (str): _description_

    Returns:
        tuple[gc_storage.Client, gc_storage.Bucket, gc_storage.Blob]: _description_

    Raises:
        google.auth.exceptions.DefaultCredentialsError: If credentials is not specified and the library fails to acquire default credentials.
        google.api_core.exceptions.GoogleAPICallError: If the API request fails for any reason.

    """
    storage_client_obj = gc_storage.Client()
    bucket_obj = storage_client_obj.bucket(bucket_nm)
    blob_obj = bucket_obj.blob(blob_nm)
    
    return storage_client_obj, bucket_obj, blob_obj

# Check if a blob exists
def check_blob_exists(bucket_nm: str, blob_nm: str, logger: logging.Logger | GCPLogger) -> bool:
    """_summary_

    Args:
        bucket_nm (str): _description_
        blob_nm (str): _description_
        logger (logging.Logger | GCPLogger): _description_

    Returns:
        bool: _description_

    Raises:
        google.auth.exceptions.DefaultCredentialsError: Propagated if credentials is not specified and the library fails to acquire default credentials.
        google.api_core.exceptions.GoogleAPICallError: Propagated if the API request fails for any reason.
    """
    _, _, blob_obj = get_blob_resources(bucket_nm, blob_nm)
    blob_path = GCSPathLib.blob_path_static(bucket_nm, blob_nm)
    
    logger.info(f"Checking if blob exists: {blob_path}")
    logger.info(f"Blob exists: {blob_obj.exists()}")
    
    return blob_obj.exists()

# Read json from gcs path
def read_json_gcs(bucket_nm: str, blob_nm: str, logger: logging.Logger | GCPLogger) -> dict[str, str] | None:
    """_summary_

    Args:
        bucket_nm (str): _description_
        blob_nm (str): _description_
        logger (logging.Logger | GCPLogger): _description_

    Returns:
        dict[str, str] | None: _description_

    Raises:
        google.auth.exceptions.DefaultCredentialsError: Propagated if credentials is not specified and the library fails to acquire default credentials.
        google.api_core.exceptions.GoogleAPICallError: Propagated if the API request fails for any reason.
        json.JSONDecodeError: If the blob content cannot be parsed as JSON
        TypeError: If the blob content is not a string or bytes-like object.
    """
    blob_path = GCSPathLib.blob_path_static(bucket_nm, blob_nm)
    
    # Get blob resources such as blob object to interact with the file
    _, _, blob_obj = get_blob_resources(bucket_nm, blob_nm)
    
    # Download the blob content as text and parse it as JSON
    logger.info(f"Reading blob content as text from: {blob_path}")
    content = blob_obj.download_as_text()
    
    return json_loads(content)

# Write json from gcs path
def write_json_gcs(data: dict, bucket_nm: str, blob_nm: str, logger: logging.Logger | GCPLogger) -> str | None:
    """_summary_

    Args:
        data (dict): _description_
        bucket_nm (str): _description_
        blob_nm (str): _description_
        logger (logging.Logger | GCPLogger): _description_

    Returns:
        str | None: _description_

    Raises:
        google.auth.exceptions.DefaultCredentialsError: If credentials is not specified and the library fails to acquire default credentials.
        google.api_core.exceptions.GoogleAPICallError: Propagated if the API request fails for any reason.
        TypeError: If the data cannot be serialized to JSON.
        ValueError: If the data contains non-serializable values.
    """
    blob_path = GCSPathLib.blob_path_static(bucket_nm, blob_nm)
    file_type = "json"
    
    # Get blob resources such as blob object to interact with the file
    _, _, blob_obj = get_blob_resources(bucket_nm, blob_nm)
    
    # Upload the JSON data as a string to the blob with the appropriate content type
    logger.info(f"Uploading JSON data to blob: {blob_path}")
    blob_obj.upload_from_string(
        json_dumps(data),
        content_type=f"application/{file_type}"
    )
    
    return f"{blob_path}"

# Read parquet from gcs path
def read_parquet_gcs(bucket_nm: str, blob_nm: str, logger: logging.Logger | GCPLogger) -> pd.DataFrame:
    """_summary_

    Args:
        bucket_nm (str): _description_
        blob_nm (str): _description_
        logger (logging.Logger | GCPLogger): _description_

    Returns:
        pd.DataFrame: _description_

    Raises:
        google.auth.exceptions.DefaultCredentialsError: Propagated if credentials is not specified and the library fails to acquire default credentials.
        google.api_core.exceptions.GoogleAPICallError: Propagated if the API request fails for any reason.
        ....
    """
    blob_path = GCSPathLib.blob_path_static(bucket_nm, blob_nm)
    read_parquet_eng_type = "auto"
    
    # Get blob resources such as blob object to interact with the file
    _, _, blob_obj = get_blob_resources(bucket_nm, blob_nm)
    
    # Download the blob content as bytes
    logger.info(f"Downloading blob content as bytes from: {blob_path}")
    parquet_bytes = blob_obj.download_as_bytes()
    buffer = BytesIO(parquet_bytes)
    
    # Read bytes into a pandas DataFrame using the specified parquet engine
    logger.info(f"Reading parquet data into DataFrame from blob: {blob_path} using engine: {read_parquet_eng_type}")
    return pd.read_parquet(buffer, engine=read_parquet_eng_type)

# Write parquet from gcs path
def write_parquet_gcs(df: pd.DataFrame, bucket_nm: str, blob_nm: str, partition_cols: Sequence[Hashable], logger: logging.Logger | GCPLogger) -> str:
    """_summary_

    Args:
        df (pd.DataFrame): _description_
        bucket_nm (str): _description_
        blob_nm (str): _description_
        partition_cols (Sequence[Hashable]): _description_
        logger (logging.Logger | GCPLogger): _description_

    Returns:
        str: _description_

    Raises:
        google.auth.exceptions.DefaultCredentialsError: If credentials is not specified and the library fails to acquire default credentials.
        google.api_core.exceptions.GoogleAPICallError: Propagated if the API request fails for any reason.
        ....
    """
    blob_path = GCSPathLib.blob_path_static(bucket_nm, blob_nm)
    save_type = "octet-stream"
    
    # Get blob resources such as blob object to interact with the file
    _, _, blob_obj = get_blob_resources(bucket_nm, blob_nm)
    
    # Write the DataFrame to a bytes buffer in parquet format
    logger.info(f"Writing DataFrame to parquet buffer for blob: {blob_path}")
    buffer = BytesIO()
    df.to_parquet(buffer, index=False, partition_cols=partition_cols)
    buffer.seek(0)
    
    # Upload the parquet data from the buffer to the blob with the appropriate content type
    logger.info(f"Uploading parquet data to blob: {blob_path}")
    blob_obj.upload_from_file(buffer, content_type=f"application/{save_type}")
    
    return f"{blob_path}"

# Convert python list to a pandas DataFrame
def list_to_df(json_lst: list, logger: logging.Logger | GCPLogger):
    """_summary_

    Args:
        json_lst (list): _description_
        logger (logging.Logger | GCPLogger): _description_

    Returns:
        _type_: _description_
    Raises:
        TypeError: If the input is not a list or if the list elements are not dicts.
        ValueError: If the list is empty or if the dicts have inconsistent keys.
    """
    
    logger.info("Converting list to pandas DataFrame")
    return pd.DataFrame(json_lst)
