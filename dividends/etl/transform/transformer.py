# Main transform logic for various data categories to GCS

# Built-in imports
from logging import Logger
from pandas import DataFrame, Series, to_datetime

# Shared imports
from shared.clients.gcp.gcs import FileConfig
from shared.clients.gcp.logging import CloudLogger
from shared.clients.gcp.services import check_blob_exists, read_parquet, write_parquet, list_to_df


# Transformation entry point
def transform(raw_json: list[dict], gcs_path_obj: FileConfig, full_refresh: bool, logger: Logger | CloudLogger) -> tuple[DataFrame, dict[str, int]]:
    """Transforms DataFrame based on specifications with caching functionality

    Args:
        raw_json (list[dict]): Data portion of JSON file
        gcs_path_obj (FileConfig): Contains necessary Google Cloud Storage data in order to save or retrieve object blobs
        full_refresh (bool): Bypasses cached data extraction files and performs API extraction
        logger (Logger | CloudLogger): Utilized for logging steps taken as well as errors

    Returns:
        tuple[DataFrame, dict[str, int]]: Transformed Pandas DataFrame + transformation metrics
    """
    
    metrics = {}
    bucket_nm = gcs_path_obj.bucket_nm
    blob_nm = gcs_path_obj.blob_nm
    
    # Check if transformed parquet file exists in GCS bucket
    tfd_file_exists = check_blob_exists(bucket_nm, blob_nm)
    metrics["transform_blob_exists"] = tfd_file_exists
    
    logger.info("RUNNING: Transformation...")
    
    # If transformed file exists and full_refresh is False, read the transformed file from GCS ie caching
    # Otherwise, transform the data and save/replace it to GCS.
    if tfd_file_exists and not full_refresh:
        tfd_df = read_transform_parquet(gcs_path_obj, logger)
        metrics["transform_blob_path"] = gcs_path_obj.blob_path
    else:
        raw_df = list_to_df(raw_json)
        tfd_df, transform_metrics = transform_main(raw_df, logger)
        metrics |= transform_metrics
        metrics["transform_blob_path"] = write_transform_parquet(tfd_df, gcs_path_obj, logger)
    
    logger.info("SUCCESS: Transformation complete.")
    
    return tfd_df, metrics

# Tranformation of the raw extracted data (cleaning, dedup, rename cols, etc)
def transform_main(df: DataFrame, logger: Logger | CloudLogger) -> tuple[DataFrame, dict[str, int]]:
    """Transforms DataFrame based on specifications

    Args:
        df (DataFrame): Untransformed Pandas DataFrame
        logger (Logger | CloudLogger): Utilized for logging steps taken as well as errors

    Returns:
        tuple[DataFrame, dict[str, int]]: Transformed Pandas DataFrame + transformation metrics
    """
    
    metrics = { "initial_row_cnt": len(df) }
    
    logger.info("Starting the main transformation process...")
    
    if not len(df):
        err_msg = "DataFrame was not provided data to transform"
        logger.warning(f"WARNING: {err_msg}")
        raise ValueError(err_msg)
    
    # Standardize column names
    df = df.rename(columns={
        "dividend": "dividend_ratio",
        "date": "market_dt",
        "payment_date": "payment_dt",
        "record_date": "record_dt",
        "declaration_date": "declar_dt"
    })
    
    # Drop duplicates
    df.drop_duplicates(inplace=True)
    metrics["post_dedup_row_cnt"] = len(df)
    
    # Drop rows with null values in critical columns
    df.dropna(subset=['symbol', 'market_dt', 'dividend_ratio'], inplace=True)
    metrics["post_dropna_row_cnt"] = len(df)
    
    # Fill nulls in non-critical columns with default values (e.g., 'Unknown' for categorical columns)
    df.get('distr_freq', Series()).fillna('Unknown', inplace=True)
    
    # Convert date columns to proper BigQuerydate format
    date_cols = ['market_dt', 'payment_dt', 'record_dt', 'declar_dt']
    for col_name in date_cols:
        if col_name in df.columns:
            df[col_name] = to_datetime(df[col_name], errors='coerce').dt.date
    
    # Reorder columns for user-friendliness
    columns_order = ['symbol', 'market_dt', 'dividend_ratio', 'distr_freq', \
                    'payment_dt', 'record_dt', 'declar_dt']
    df = df.reindex(columns=[c for c in columns_order if c in df.columns])
    
    metrics["final_row_cnt"] = len(df)
    
    logger.info("Main transformation process completed.")
    
    return df, metrics

# Read the transformed parquet from GCS bucket
def read_transform_parquet(gcs_path_obj: FileConfig, logger: Logger | CloudLogger) -> DataFrame:
    """Read transformed parquet file from GCS bucket

    Args:
        gcs_path_obj (FileConfig): Contains necessary Google Cloud Storage data in order to save or retrieve object blobs
        logger (Logger | CloudLogger): Utilized for logging steps taken as well as errors

    Returns:
        DataFrame: Transformed Pandas DataFrame from cached parquet file
    """
    
    logger.info("Reading transformed file from GCS bucket...")
    logger.info(gcs_path_obj)
    
    # Read parquet from GCS bucket
    try:
        tfd_df = read_parquet(gcs_path_obj.bucket_nm, gcs_path_obj.blob_nm,)
    except Exception as e:
        err_msg = "Failed to read transformed file from GCS"
        logger.error(f"ERROR: {err_msg} - REASON: {e}")
        raise ValueError(err_msg)
    
    logger.info("Transformed file was read.")
    return tfd_df

# Write the transformed parquet from GCS bucket
def write_transform_parquet(df: DataFrame, gcs_path_obj: FileConfig, logger: Logger | CloudLogger) -> str:
    """Write transformed parquet file to GCS bucket

    Args:
        df (DataFrame): Transformed Pandas DataFrame
        gcs_path_obj (FileConfig): Contains necessary Google Cloud Storage data in order to save or retrieve object blobs
        logger (Logger | CloudLogger): Utilized for logging steps taken as well as errors

    Returns:
        str: Path to where GCS blob was saved
    """
    
    logger.info("Writing transformed file to GCS bucket...")
    logger.info(gcs_path_obj)
    
    # Write parquet to GCS bucket
    try:
        blob_path = write_parquet(df, gcs_path_obj.bucket_nm, gcs_path_obj.blob_nm)
    except Exception as e:
        err_msg = "Failed to write transformed file to GCS"
        logger.error(f"ERROR: {err_msg} - REASON: {e}")
        raise ValueError(err_msg)
    
    logger.info("Transformed file was written.")
    
    return blob_path
