# Main transform logic for various data categories to GCS

# Built-in imports
import pandas as pd
import logging

# Shared imports
from shared.clients.gcp.logging import GCPLogger
from shared.clients.gcp.naming_conv import GCSPathLib
from shared.clients.gcp.services import check_blob_exists, read_parquet_gcs, write_parquet_gcs, list_to_df


def transform(raw_json: list[dict], gcs_path_obj: GCSPathLib, full_refresh: bool, logger: GCPLogger) -> tuple[pd.DataFrame, dict[str, str]]:
    metrics = {}
    bucket_nm, blob_nm = gcs_path_obj.bucket_blob_nms()
    
    tfd_file_exists = check_blob_exists(bucket_nm, blob_nm, logger)
    metrics["transform_file_exists"] = tfd_file_exists
    
    if tfd_file_exists:
        tfd_df = read_parquet_gcs(bucket_nm, blob_nm, logger)
    else:
        raw_df = list_to_df(raw_json, logger)
        tfd_df, transform_metrics = transform_main(raw_df, logger)
        metrics.update(transform_metrics)
        write_parquet_gcs(tfd_df, bucket_nm, blob_nm, logger)
    
    return tfd_df, metrics


def transform_main(df: pd.DataFrame, logger: logging.Logger | GCPLogger) -> tuple[pd.DataFrame, dict]:
    metrics = { "initial_row_cnt": len(df) }
    
    if not len(df):
        logger.error("df has no data")
        raise ValueError("df has no data")
    
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
    df.get('distr_freq', pd.Series()).fillna('Unknown', inplace=True)
    
    # Convert date columns to proper BigQuerydate format
    date_cols = ['market_dt', 'payment_dt', 'record_dt', 'declar_dt']
    for col_name in date_cols:
        if col_name in df.columns:
            df[col_name] = pd.to_datetime(df[col_name], errors='coerce').dt.date
    
    # Reorder columns for user-friendliness
    columns_order = ['symbol', 'market_dt', 'dividend_ratio', 'distr_freq', \
                    'payment_dt', 'record_dt', 'declar_dt']
    df = df.reindex(columns=[c for c in columns_order if c in df.columns])
    
    metrics["final_row_cnt"] = len(df)
    return df, metrics

def read_parquet_from_gcs(bucket_nm: str, blob_nm: str, logger: GCPLogger) -> pd.DataFrame:
    # Read the transformed file from GCS and return it as a pandas DataFrame
    logger.info("Reading transformed file from GCS...")
    tfd_df = read_parquet_gcs(bucket_nm, blob_nm, logger)
    
    if tfd_df is None:
        err_msg = "ERROR: Transformed file was not read."
        logger.error(err_msg)
        raise ValueError(err_msg)
    
    logger.info("SUCCESS: Transformed file was read.")
    return tfd_df
