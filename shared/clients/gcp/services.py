# GCP services client wrapper to interact with various GCP services like Secret Manager, Cloud Storage, and Dataproc

# Built-in imports
from io import BytesIO
from json import dumps as json_dumps, loads as json_loads
from pandas import DataFrame, read_parquet as pandas_read_parquet

# Shared imports
from shared.clients.gcp.gcs import FileConfig

# Google API imports
from google.cloud import bigquery as bq
from google.cloud import storage as gcs
from google.api_core.exceptions import Conflict as ConflictError



############################## BigQuery Services ##############################

def create_target_table(tgt_ds: str, tgt_table: str, col_metadata: list[tuple[str, ...]], part_col: str | None, part_gran: str | None, clust_cols: list[str] | None) -> None:
    """Creates the target/main table
    
    Args:
        bq_client (bq.Client): BigQuery Client object
        tgt_bq_table_obj (TableConfig): Contains necessary Google BigQuery table metadata for the target table
        part_col (str | None): Partition columns for the target table
        clust_cols (list[str] | None):
        logger (Logger | CloudLogger): Utilized for logging steps taken as well as errors
    
    Returns: None
    """
    bq_client: bq.Client = bq.Client()
    
    tgt_ds_tbl: str = f"{tgt_ds}.{tgt_table}"
    optional_clause: str = ""
    
    # Create table with data types/constraints
    create_tbl_clause = ", ".join(" ".join(col) for col in col_metadata)
    
    # Add partitioning to the create table query if the respective parameter is provided
    if part_col is not None and part_gran is not None:
        optional_clause = f" PARTITION BY DATE_TRUNC({part_col}, {part_gran})"
    else: 
        raise ValueError("ERROR: Unsupported partition type. REASON: Date partition fields only.")
    
    # Add clustering clauses to the create table query if the respective parameter is provided
    optional_clause += f" CLUSTER BY {', '.join(clust_cols)}" if clust_cols is not None else ""
    
    create_tbl_query: str = \
        f"""
            CREATE TABLE IF NOT EXISTS {tgt_ds_tbl} (
                {create_tbl_clause}
            )  
            {optional_clause}
        """
    
    # Run create table query and wait for it to finish
    create_tbl_query_job: bq.QueryJob = bq_client.query(create_tbl_query)
    create_tbl_query_job.result()
    
    # Log errors and raise exception
    if create_tbl_query_job.error_result:
        err_msg: str = create_tbl_query_job.error_result.get("message", "Unknown error")
        err_reason: str = create_tbl_query_job.error_result.get("reason", "Unknown reason for error")
        
        raise ConflictError(f"ERROR: {err_msg} - REASON: {err_reason}")

def create_staging_table(tgt_ds: str, tgt_table: str, stg_ds: str, stg_table: str) -> None:
    """Creates the dividend category's staging/temp table
    
    Args:
        bq_client (bq.Client): BigQuery Client object
        tgt_bq_table_obj (TableConfig): Contains necessary Google BigQuery table metadata for the target table
        stg_bq_table_obj (TableConfig): Contains necessary Google BigQuery table metadata for the staging table
        logger (Logger | CloudLogger): Utilized for logging steps taken as well as errors
    
    Returns: None
    """
    tgt_ds_tbl: str = f"{tgt_ds}.{tgt_table}"
    stg_ds_tbl: str = f"{stg_ds}.{stg_table}"
    
    bq_client: bq.Client = bq.Client()
    
    # Create or replace staging table query w/ same schema as target table
    create_tbl_query: str = \
        f"""
            CREATE OR REPLACE TABLE {stg_ds_tbl} 
            AS 
            SELECT * FROM {tgt_ds_tbl} WHERE 1 = 0 
        """
    
    # Run create table query and wait for it to finish
    create_tbl_query_job: bq.QueryJob = bq_client.query(create_tbl_query)
    create_tbl_query_job.result()
    
    # Log errors and raise exception
    if create_tbl_query_job.error_result:
        err_msg: str = create_tbl_query_job.error_result.get("message", "Unknown error")
        err_reason: str = create_tbl_query_job.error_result.get("reason", "Unknown reason for error")
        
        raise ConflictError(f"ERROR: {err_msg} - REASON: {err_reason}")

def load_table(df: DataFrame, ds: str, table: str, write_disp: str) -> None:
    """Loads transformed relational data into a staging/temp table

    Args:
        df (DataFrame): Transformed Pandas DataFrame
        bq_client (bq.Client): BigQuery Client object
        stg_bq_table_obj (TableConfig): Contains necessary Google BigQuery table metadata for the staging table
        logger (Logger | CloudLogger): Utilized for logging steps taken as well as errors

    Returns: None
    """
    
    job_config: bq.LoadJobConfig = bq.LoadJobConfig(write_disposition=write_disp)
    stg_ds_tbl: str = f"{ds}.{table}"
    
    bq_client: bq.Client = bq.Client()
    
    # Load data from the provided pandas DataFrame to the staging table and wait for the load job to finish
    load_tbl_query_job: bq.LoadJob = bq_client.load_table_from_dataframe(df, stg_ds_tbl, job_config=job_config)
    load_tbl_query_job.result()
    
    # Log errors and raise exception
    if load_tbl_query_job.error_result:
        err_msg = load_tbl_query_job.error_result.get("message", "Unknown error")
        err_reason = load_tbl_query_job.error_result.get("reason", "Unknown reason for error")
        
        raise ConflictError(f"ERROR: {err_msg} - REASON: {err_reason}")

def merge_table(tgt_ds: str, tgt_table: str, stg_ds: str, stg_table: str, all_cols: list[str], join_cols: list[str]) -> None:
    """Merge staging/temp table into target table while ensuring duplicate records are not inserted

    Args:
        bq_client (bq.Client): BigQuery Client object
        tgt_bq_table_obj (TableConfig): Contains necessary Google BigQuery table metadata for the target table
        stg_bq_table_obj (TableConfig): Contains necessary Google BigQuery table metadata for the staging table
        logger (Logger | CloudLogger): Utilized for logging steps taken as well as errors

    Returns: None
    """
    
    tgt_ds_tbl: str = f"{tgt_ds}.{tgt_table}"
    stg_ds_tbl: str = f"{stg_ds}.{stg_table}"
    
    bq_client: bq.Client = bq.Client()
    
    # Required clause values for merging properly and ensuring no duplicates
    join_clause = " AND ".join(f"target.{col} = staging.{col}" for col in join_cols)
    update_clause  = ", ".join(f"target.{col} = staging.{col}" for col in all_cols if col not in join_cols)
    insert_columns = ", ".join(all_cols)
    insert_values  = ", ".join(f"staging.{col}" for col in all_cols)
    
    merge_tbls_query = \
        f"""
            MERGE INTO `{tgt_ds_tbl}` AS target
            USING `{stg_ds_tbl}` AS staging
            ON {join_clause}
            WHEN MATCHED THEN
            UPDATE SET {update_clause}
            WHEN NOT MATCHED THEN
            INSERT ({insert_columns})
            VALUES ({insert_values});
        """
    
    # Execute the merge query and wait for it to complete
    merge_tbls_query_job: bq.QueryJob = bq_client.query(merge_tbls_query)
    merge_tbls_query_job.result()
    
    # Log errors and raise exception
    if merge_tbls_query_job.error_result:
        err_msg = merge_tbls_query_job.error_result.get("message", "Unknown error")
        err_reason = merge_tbls_query_job.error_result.get("reason", "Unknown reason for error")
        
        raise ConflictError(f"ERROR: {err_msg} - REASON: {err_reason}")


############################ Cloud Storage Services ############################


def get_blob_obj(bucket_nm: str, blob_nm: str) -> gcs.Blob:
    """Get blob object"""
    
    storage_client_obj = gcs.Client()
    bucket_obj = storage_client_obj.bucket(bucket_nm)
    blob_obj = bucket_obj.blob(blob_nm)
    
    return blob_obj

def check_blob_exists(bucket_nm: str, blob_nm: str) -> bool:
    """Check if a blob exists"""
    
    blob_obj = get_blob_obj(bucket_nm, blob_nm)
    blob_path = FileConfig.blob_path_static(bucket_nm, blob_nm)
    
    return blob_obj.exists()

def read_json(bucket_nm: str, blob_nm: str) -> dict:
    """Read json from gcs path"""
    
    blob_path = FileConfig.blob_path_static(bucket_nm, blob_nm)
    blob_obj = get_blob_obj(bucket_nm, blob_nm)
    
    # Download the blob content as text
    content = blob_obj.download_as_text()
    
    return json_loads(content)

def write_json(data: dict, bucket_nm: str, blob_nm: str) -> str:
    """Write json from gcs path"""
    
    file_type = "json"
    
    blob_path = FileConfig.blob_path_static(bucket_nm, blob_nm)
    blob_obj = get_blob_obj(bucket_nm, blob_nm)
    
    # Upload the JSON data as a string to the blob
    blob_obj.upload_from_string(
        json_dumps(data),
        content_type=f"application/{file_type}"
    )
    
    return blob_path

def read_parquet(bucket_nm: str, blob_nm: str) -> DataFrame:
    """Read parquet from gcs path"""
    
    read_parquet_eng_type = "auto"
    
    blob_path = FileConfig.blob_path_static(bucket_nm, blob_nm)
    blob_obj = get_blob_obj(bucket_nm, blob_nm)
    
    # Download the blob content as bytes and store to buffer
    parquet_bytes = blob_obj.download_as_bytes()
    buffer = BytesIO(parquet_bytes)
    
    # Read bytes into a pandas DataFrame using the specified parquet engine
    return pandas_read_parquet(buffer, engine=read_parquet_eng_type)

def write_parquet(df: DataFrame, bucket_nm: str, blob_nm: str) -> str:
    """Write parquet from gcs path"""
    
    save_type = "octet-stream"
    engine_type = "pyarrow"
    
    blob_path = FileConfig.blob_path_static(bucket_nm, blob_nm)
    blob_obj = get_blob_obj(bucket_nm, blob_nm)
    
    buffer = BytesIO()
    df.to_parquet(buffer, index=False, engine=engine_type)
    buffer.seek(0)
    
    blob_obj.upload_from_file(buffer, content_type=f"application/{save_type}")
    
    return blob_path

def list_to_df(json_lst: list) -> DataFrame:
    """Convert python list to a pandas DataFrame"""
    
    return DataFrame(json_lst)
