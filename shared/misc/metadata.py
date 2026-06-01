# Metadata holder for ETL process. This can be used to store any metadata related to the ETL process, such as data source information, transformation logic, etc.
## ms_metadata table in bigquery can be used to store metadata related to the ETL process. This table can have columns such as data_type, source_url, api_key_used, transformation_logic, load_target, etc. This will help in tracking the ETL process and also in debugging any issues that may arise during the process. We can also have a separate table for each data category if needed, but for now we will have a single table to store metadata for all data categories.
# column names
## dat_cat (dividends, etc.)
## status   (success, failure, running, waiting, etc.)
## batch_dt (date of the batch run)
## start_dt (start date of the data extraction)
## end_dt (end date of the data extraction)
## start_time (timestamp when the batch started)
## end_time (timestamp when the batch ended)
## symbols (array of symbols for which the data was extracted)
## hash (hash value of the input parameters for the batch run, can be used for id

# Built-in imports
from datetime import datetime, timezone
from logging import Logger

# Shared imports
from shared.clients.gcp.logging import CloudLogger

# Google API imports
from google.cloud import bigquery as bq


# TODO: incorporation still in development
# BigQuery table that holds metadata regarding the whole ETL pipeline such as which steps have succeeded and parameters
def metadata(data_type, dataset_nm, batch_dt, start_dt, end_dt, logger: Logger | CloudLogger):
    bq_client = bq.Client()
    
    create_ms_metadata_tbl_job = create_ms_metadata_tbl(bq_client, dataset_nm)
    create_ms_metadata_tbl_job.result()
    if create_ms_metadata_tbl_job.error_result:  
        logger.error(f"{create_ms_metadata_tbl_job.error_result.reason}: {create_ms_metadata_tbl_job.error_result.message}"); return False
    
    chk_if_ms_metadata_exists_job = chk_if_ms_metadata_exists(bq_client, data_type, dataset_nm, batch_dt)
    chk_if_ms_metadata_exists_res = chk_if_ms_metadata_exists_job.result()
    if chk_if_ms_metadata_exists_job.error_result:  
                logger.error(f"{chk_if_ms_metadata_exists_job.error_result.reason}: {chk_if_ms_metadata_exists_job.error_result.message}"); return False
    if chk_if_ms_metadata_exists_res.total_rows is None or chk_if_ms_metadata_exists_res.total_rows > 0: 
        logger.error(f"Query not yet complete, the result set size is unknown OR Record for BATCH DATE: {batch_dt} for {data_type} data exists already"); return False
    
    insert_ms_metadata_job = insert_ms_metadata(bq_client, data_type, dataset_nm, batch_dt, start_dt, end_dt)
    if insert_ms_metadata_job.error_result:  
        logger.error(f"{insert_ms_metadata_job.error_result.reason}: {insert_ms_metadata_job.error_result.message}"); return False
    if insert_ms_metadata_job.num_dml_affected_rows != 1:
        logger.error(f"Unable to insert record into {dataset_nm}.ms_{data_type.lower()}"); return False
    
    logger.info(f"Inserted record into {dataset_nm}.ms_{data_type.lower()}")
    return True

# Create the metadata table
def create_ms_metadata_tbl(bq_client: bq.Client, dataset_nm: str):
    create_tbl_query = \
        f"""
            CREATE TABLE IF NOT EXISTS {dataset_nm}.ms_metadata (
                data_type    STRING         NOT NULL,
                status      STRING         NULLABLE,
                batch_dt    DATE           NOT NULL,
                start_dt    DATE           NOT NULL,
                end_dt      DATE           NOT NULL,
                start_time  TIMESTAMP      NULLABLE,
                end_time    TIMESTAMP      NULLABLE,
                symbols     ARRAY<STRING>  NOT NULL,
                hash        STRING         NOT NULL
            )
        """
    
    create_tbl_query_job = bq_client.query(create_tbl_query)
    create_tbl_query_job.result()
    
    return create_tbl_query_job

# TODO: need to fix
# Checks if metadata record exists......
def chk_if_ms_metadata_exists(bq_client: bq.Client, data_typee: str, dataset_nm: str, batch_dt):
    if_exists_query = \
        f"""
            SELECT batch_dt 
            FROM   {dataset_nm}.ms_metadata 
            WHERE  data_type = "{data_type}"  AND  batch_dt = {batch_dt}
        """
    
    if_exists_query_job = bq_client.query(if_exists_query)
    if_exists_query_job.result()
    
    return if_exists_query_job

# TODO:
# ........
def insert_ms_metadata(bq_client: bq.Client, data_type: str, dataset_nm: str, batch_dt, start_dt, end_dt):
    query_insert = \
        f"""
            INSERT INTO {dataset_nm}.ms_metadata 
            (
                data_type,
                status,
                batch_dt,
                start_dt,
                end_dt,
                start_time,
                end_time,
                symbols,
                hash
            )
            VALUES (
                {data_type},
                'WAITING',
                {batch_dt},
                {start_dt},
                {end_dt},
                {datetime.now(timezone.utc)},
                NULL,
                NULL
            )
        """
    
    query_insert_job = bq_client.query(query_insert)
    query_insert_job.result()
    
    return query_insert_job