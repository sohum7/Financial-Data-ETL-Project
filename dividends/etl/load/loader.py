# Main load logic for various data categories to BigQuery

# Built-in imports
from pandas import DataFrame as pd_DataFrame
import logging


# Shared imports
from shared.clients.gcp.logging import GCPLogger
#from shared.clients.gcp.naming_conv import GCSPathLib

# Google API imports
from google.api_core.exceptions import Conflict
from google.cloud import bigquery as gc_bigquery


def load(df_or_uri: pd_DataFrame | str, tgt_ds_tbl: str, stg_ds_tbl: str, logger: GCPLogger) -> None:
    # Initialize BigQuery client
    bq_client = gc_bigquery.Client()
    
    # Create dividends target table if it doesn't exist
    create_dividends_tgt_tbl(bq_client, tgt_ds_tbl, "market_dt", "symbol", "market_dt")
    
    # Create or replace dividends staging table
    create_stg_tbl(bq_client, tgt_ds_tbl, stg_ds_tbl)
    
    # Load data to dividends staging table using the provided load function (either from df or uri)
    load_by_data_type(bq_client, df_or_uri, stg_ds_tbl, logger)


def load_by_data_type(bq_client: gc_bigquery.Client, df_or_uri: pd_DataFrame | str, stg_ds_tbl: str, logger: GCPLogger) -> None:
    if isinstance(df_or_uri, pd_DataFrame):
        logger.info("Passing a pandas DataFrame to load function")
        load_df_to_stg_tbl(df_or_uri, bq_client, stg_ds_tbl)
    elif isinstance(df_or_uri, str):
        logger.info(f"Passing a uri to load function: {df_or_uri}")
        load_uri_to_stg_tbl(df_or_uri, bq_client, stg_ds_tbl)

def create_dividends_tgt_tbl(bq_client: gc_bigquery.Client, ds_tbl: str, partition_col: str | None=None, *cluster_cols: str) -> gc_bigquery.QueryJob:
    optional_clause = ""
    
    # Add partitioning to the create table query if the respective parameter is provided
    if partition_col:
        optional_clause = f" PARTITION BY {partition_col} "
    
    # Add clustering clauses to the create table query if the respective parameter is provided
    if cluster_cols:
        cluster_cols_str = ", ".join( col.strip() for col in cluster_cols )
        optional_clause += f" CLUSTER BY {cluster_cols_str} "
    
    # Create table if not exists query
    create_tbl_query = \
        f"""
            CREATE TABLE IF NOT EXISTS {ds_tbl} (
                symbol STRING,
                market_dt DATE,
                dividend_ratio FLOAT64,
                distr_freq STRING,
                payment_dt DATE,
                record_dt DATE,
                declar_dt DATE
            )  
            {optional_clause}
        """
    
    # Run create table query and wait for it to finish
    create_tbl_query_job = bq_client.query(create_tbl_query)
    create_tbl_query_job.result()
    
    # If there was an error creating the table, raise an exception with the error details
    if create_tbl_query_job.error_result:
        err_msg = f"Error creating target table: {create_tbl_query_job.error_result}"
        raise Conflict(err_msg)
    
    return create_tbl_query_job

def create_stg_tbl(bq_client: gc_bigquery.Client, tgt_ds_tbl: str, stg_ds_tbl: str) -> gc_bigquery.QueryJob:
    # Ensure that the provided target and staging dataset.table parameters are in the correct format before running the create table query
    if "." not in tgt_ds_tbl or "." not in stg_ds_tbl: 
        logging.error(f"create_stg_tbl was not provided tgt_ds_tbl and stg_ds_tbl parameter's with dataset and table names as such 'ds_nm.tbl_nm' ")
    
    # Create or replace staging table query w/ same schema as target table
    create_tbl_query = \
        f"""
            CREATE OR REPLACE TABLE {stg_ds_tbl} 
            LIKE {tgt_ds_tbl}
        """
    
    # Run create table query and wait for it to finish
    create_tbl_query_job = bq_client.query(create_tbl_query)
    create_tbl_query_job.result()
    
    # If there was an error creating the table, raise an exception with the error details
    if create_tbl_query_job.error_result:
        err_msg = f"Error creating {stg_ds_tbl} staging table: {create_tbl_query_job.error_result}"
        raise Conflict(err_msg)
    
    return create_tbl_query_job

def load_df_to_stg_tbl(df: pd_DataFrame, bq_client: gc_bigquery.Client, stg_ds_tbl: str):
    # Load job configuration with write disposition set to overwrite the staging table data with each load
    job_config = gc_bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    
    # Load data from the provided pandas DataFrame to the staging table and wait for the load job to finish
    load_tbl_query_job = bq_client.load_table_from_dataframe(df, stg_ds_tbl, job_config=job_config)
    load_tbl_query_job.result()
    
    # If there was an error loading the data to the staging table, raise an exception with the error details
    if load_tbl_query_job.errors:
        err_msg = f"Error loading data to {stg_ds_tbl} staging table{'.' if not load_tbl_query_job else f': {load_tbl_query_job.errors}'}"
        raise Conflict(err_msg)

def load_uri_to_stg_tbl(uri: str, bq_client: gc_bigquery.Client, stg_ds_tbl: str):
    # Extract the file type from the provided URI to specify the correct format in the load data query
    file_type = uri.split(".")[-1]
    
    # Load data query from GCS URI
    load_tbl_query = \
        f"""
            LOAD DATA INTO {stg_ds_tbl}
            FROM FILES (
            format = '{file_type.upper()}',
            uris = ['{uri}']
            )
        """
    
    # Run load data query and wait for it to finish
    load_tbl_query_job = bq_client.query(load_tbl_query)
    load_tbl_query_job.result()
    
    # If there was an error loading the data to the staging table, raise an exception with the error details
    if load_tbl_query_job.errors:
        err_msg = f"Error loading data to {stg_ds_tbl} staging table{'.' if not load_tbl_query_job else f': {load_tbl_query_job.errors}'}"
        raise Conflict(err_msg)
