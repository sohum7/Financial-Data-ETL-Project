# Main load logic for various data categories to BigQuery

# Shared imports
from google.cloud import bigquery as gc_bigquery
from shared.clients.gcp.naming_conv import MS_FILE_NM, GCS_DIR_PATH, DF_SAVE_PATH
from shared.misc.utilities import http_return


def load(data_cat, bucket_dir, tgt_ds_tbl, stg_ds_tbl, tfd_file_type, logger, **kwargs):
    bq_client = gc_bigquery.Client()
    
    try:
        bq_client = gc_bigquery.Client()
        
        # Create or replace staging table
        create_stg_tbl_job = create_stg_tbl(bq_client, tgt_ds_tbl, stg_ds_tbl)
        if create_stg_tbl_job.error_result:
            msg = f"Error creating {data_cat.upper()}'s BQ staging table: {create_stg_tbl_job.error_result}"
            logger.error(msg)
            return http_return(500, msg)
        
        #gcs_file_path = GCS_FILE_PATH(batch_dt, bucket_nm, bucket_dir_path, f"{file_nm}*.parquet")  # Load all parquet files for this batch into the staging table
        load_to_stg_tbl_job = load_to_stg_tbl(bq_client, bucket_dir, stg_ds_tbl, tfd_file_type)
        if load_to_stg_tbl_job.errors:
            msg = f"Error loading data to {data_cat.upper()}'s BQ staging table: {load_to_stg_tbl_job.errors}"
            logger.error(msg)
            return http_return(500, msg)
    
    except Exception as e:
        msg = f"Error loading data to {data_cat.upper()}'s BQ staging table"
        logger.error(msg)
        return http_return(500, msg)
    
    msg = f"Loading to {data_cat.upper()}'s BQ staging table is complete"
    logger.info(msg)
    return http_return(200, msg)

def create_stg_tbl(bq_client, tgt_dataset_tbl, stg_dataset_tbl):
    create_tbl_query = \
        f"""
            CREATE OR REPLACE TABLE {stg_dataset_tbl} 
            LIKE {tgt_dataset_tbl}
        """
    
    create_tbl_query_job = bq_client.query(create_tbl_query)
    create_tbl_query_job.result()
    
    return create_tbl_query_job

def load_to_stg_tbl(bq_client, bucket_dir, stg_ds_tbl, file_type):
    
    load_tbl_query = \
        f"""
            LOAD DATA INTO {stg_ds_tbl}
            FROM FILES (
            format = '{file_type.upper()}',
            uris = ['{bucket_dir}*.{file_type.lower()}']
            )
        """
    
    load_tbl_query_job = bq_client.query(load_tbl_query)
    load_tbl_query_job.result()
    
    return load_tbl_query_job


