# Metadata holder for ETL process. This can be used to store any metadata related to the ETL process, such as data source information, transformation logic, etc.
## ms_metadata table in bigquery can be used to store metadata related to the ETL process. This table can have columns such as data_category, source_url, api_key_used, transformation_logic, load_target, etc. This will help in tracking the ETL process and also in debugging any issues that may arise during the process. We can also have a separate table for each data category if needed, but for now we will have a single table to store metadata for all data categories.
# column names
## dat_cat (dividends, ticketers, etc.)
## batch_dt (date of the batch run)
## start_dt (start date of the data extraction)
## end_dt (end date of the data extraction)
## extract_status   (success, failure, running, retrying, waiting, etc.)
## transform_status (success, failure, running, retrying, waiting, etc.)
## load_status      (success, failure, running, retrying, waiting, etc.)
## start_time (timestamp when the batch started)
## end_time (timestamp when the batch ended)

# Builtin imports
from datetime import datetime
import pytz

# Shared imports
from google.cloud import bigquery as bq
from shared.clients.gcp_logging import GCPLogger


def metadata(data_cat, dataset_nm, batch_dt, start_dt, end_dt, logger: GCPLogger, **kwargs):
    bq_client = bq.Client()
    
    create_ms_metadata_tbl_job = create_ms_metadata_tbl(bq_client, dataset_nm)
    if create_ms_metadata_tbl_job.error_result:  
        logger.error(f"{create_ms_metadata_tbl_job.error_result.reason}: {create_ms_metadata_tbl_job.error_result.message}"); return False
    
    chk_if_ms_metadata_exists_job = chk_if_ms_metadata_exists(bq_client, data_cat, dataset_nm, batch_dt)
    chk_if_ms_metadata_exists_res = chk_if_ms_metadata_exists_job.result()
    if chk_if_ms_metadata_exists_job.error_result:  
                logger.error(f"{chk_if_ms_metadata_exists_job.error_result.reason}: {chk_if_ms_metadata_exists_job.error_result.message}"); return False
    if chk_if_ms_metadata_exists_res.total_rows is None or chk_if_ms_metadata_exists_res.total_rows > 0: 
        logger.error(f"Query not yet complete, the result set size is unknown OR Record for BATCH DATE: {batch_dt} for {data_cat} data exists already"); return False
    
    insert_ms_metadata_job = insert_ms_metadata(bq_client, data_cat, dataset_nm, batch_dt, start_dt, end_dt)
    if insert_ms_metadata_job.error_result:  
        logger.error(f"{insert_ms_metadata_job.error_result.reason}: {insert_ms_metadata_job.error_result.message}"); return False
    if insert_ms_metadata_job.num_dml_affected_rows != 1:
        logger.error(f"Unable to insert record into {dataset_nm}.ms_{data_cat.lower()}"); return False
    
    logger.info(f"Inserted record into {dataset_nm}.ms_{data_cat.lower()}")
    return True

def create_ms_metadata_tbl(bq_client: bq.Client, dataset_nm: str):
    create_tbl_query = \
        f"""
            CREATE TABLE IF NOT EXISTS {dataset_nm}.ms_metadata (
                data_cat STRING,
                batch_dt DATE,
                start_dt DATE,
                end_dt DATE,
                extract_status STRING,
                transform_status STRING,
                load_status STRING,
                start_time TIMESTAMP,
                end_time TIMESTAMP
            )
        """
    
    create_tbl_query_job = bq_client.query(create_tbl_query)
    create_tbl_query_job.result()
    
    return create_tbl_query_job

def chk_if_ms_metadata_exists(bq_client: bq.Client, data_cat: str, dataset_nm: str, batch_dt):
    if_exists_query = \
        f"""
            SELECT batch_dt 
            FROM   {dataset_nm}.ms_metadata 
            WHERE  data_cat = "{data_cat}"  AND  batch_dt = {batch_dt}
        """
    
    if_exists_query_job = bq_client.query(if_exists_query)
    if_exists_query_job.result()
    
    return if_exists_query_job

def insert_ms_metadata(bq_client: bq.Client, data_cat: str, dataset_nm: str, batch_dt, start_dt, end_dt):
    query_insert = \
        f"""
            INSERT INTO {dataset_nm}.ms_metadata 
            VALUES (
                {batch_dt},
                {start_dt},
                {end_dt},
                'WAITING',
                'WAITING',
                'WAITING',
                {datetime.now(pytz.utc)},
                NULL
            )
        """
    
    query_insert_job = bq_client.query(query_insert)
    query_insert_job.result()
    
    return query_insert_job