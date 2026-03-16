# Main load logic for various data categories to BigQuery

# Built-in imports
from pandas import DataFrame as pd_DataFrame
import logging

# Shared imports
from shared.clients.gcp.logging import GCPLogger
from shared.clients.gcp.naming_conv import GCSPathLib

# Google API imports
from google.api_core.exceptions import Conflict
from google.cloud import bigquery as gc_bigquery

def load(df_or_uri, tgt_ds_tbl, stg_ds_tbl, logger: GCPLogger):
    if isinstance(df_or_uri, pd_DataFrame):
        logger.info("Passing a pandas DataFrame to load function")
        load_main(df_or_uri, load_df_to_stg_tbl, tgt_ds_tbl, stg_ds_tbl, logger)
    elif isinstance(df_or_uri, str):
        logger.info(f"Passing a uri to load function: {df_or_uri}")
        load_main(df_or_uri, load_uri_to_stg_tbl, tgt_ds_tbl, stg_ds_tbl, logger)

def load_main(df_or_uri, load_stg_tbl_func, tgt_ds_tbl, stg_ds_tbl, logger: GCPLogger):
    try:
        bq_client = gc_bigquery.Client()
        #bq_client.load_table_from_dataframe # remove
        
        create_tgt_tbl_job = create_dividends_tgt_tbl(bq_client, tgt_ds_tbl, "market_dt", True, "symbol", "market_dt")
        if create_tgt_tbl_job.error_result:
            err_msg = f"Error creating target table: {create_tgt_tbl_job.error_result}"
            return Conflict(err_msg)
        
        create_stg_tbl_job = create_stg_tbl(bq_client, tgt_ds_tbl, stg_ds_tbl)
        if create_stg_tbl_job.error_result:
            err_msg = f"Error creating {stg_ds_tbl} staging table: {create_stg_tbl_job.error_result}"
            raise Conflict(err_msg)
        
        load_to_stg_tbl_job = load_stg_tbl_func(df_or_uri, bq_client, stg_ds_tbl)
        if load_to_stg_tbl_job is None or load_to_stg_tbl_job.errors:
            err_msg = f"Error loading data to {stg_ds_tbl} staging table{'.' if not load_to_stg_tbl_job else f': {load_to_stg_tbl_job.errors}'}"
            raise Conflict(err_msg)
        
    except Conflict as e:
        logger.error(e.message)
    except Exception as e:
        logger.error(f"Error loading data to {stg_ds_tbl} staging table: {e}")
    else:
        logger.info(f"Loading to {stg_ds_tbl} staging table is complete.")
        return True
    
    return False

def create_dividends_tgt_tbl(bq_client: gc_bigquery.Client, ds_tbl: str, part_col: str | None=None, part_col_is_dt: bool=False, *cluster_cols: str):
    optional_clause = ""
    
    if part_col:
        tmp_optional_clause = f"DATE({part_col})" if part_col_is_dt else part_col
        optional_clause = f" PARTITION BY {tmp_optional_clause} "
    
    if cluster_cols:
        cluster_cols_str = ", ".join( col.strip() for col in cluster_cols )
        optional_clause += f" CLUSTER BY ({cluster_cols_str}) "
    
    create_tbl_query = \
        f"""
            CREATE TABLE IF NOT EXISTS {ds_tbl} (
                symbol STRING,
                market_dt DATE,
                dividend_ratio DOUBLE,
                distr_freq STRING,
                payment_dt DATE,
                record_dt DATE,
                declar_dt DATE
            )  
            {optional_clause}
        """
    
    create_tbl_query_job = bq_client.query(create_tbl_query)
    create_tbl_query_job.result()
    
    return create_tbl_query_job

def create_stg_tbl(bq_client, tgt_ds_tbl, stg_ds_tbl):
    if "." not in tgt_ds_tbl or "." not in stg_ds_tbl: logging.error(f"create_stg_tbl was not provided tgt_ds_tbl and stg_ds_tbl parameter's with dataset and table names as such 'ds_nm.tbl_nm' ")

    create_tbl_query = \
        f"""
            CREATE OR REPLACE TABLE {stg_ds_tbl} 
            LIKE {tgt_ds_tbl}
        """
    
    create_tbl_query_job = bq_client.query(create_tbl_query)
    create_tbl_query_job.result()
    
    return create_tbl_query_job

def load_df_to_stg_tbl(df, bq_client, stg_ds_tbl):
    if df is None:
        logging.error(f"df is None")
        return None
    if not len(df):
        logging.error(f"0 records in df")
    else:
        job_config = gc_bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        
        load_tbl_query_job = bq_client.load_table_from_dataframe(df, stg_ds_tbl, job_config)
        load_tbl_query_job.result()
    
    return load_tbl_query_job

def load_uri_to_stg_tbl(uri, bq_client, stg_ds_tbl):
    if "." not in uri: 
        logging.error(f"load_to_stg_tbl was not provided file type from uri parameter")
        return None
    
    file_type = uri.split(".")[-1]
    
    load_tbl_query = \
        f"""
            LOAD DATA INTO {stg_ds_tbl}
            FROM FILES (
            format = '{file_type.upper()}',
            uris = ['{uri}']
            )
        """
    
    load_tbl_query_job = bq_client.query(load_tbl_query)
    load_tbl_query_job.result()
    
    return load_tbl_query_job
