# 

# Shared imports
from shared.clients.gcp.logging import GCPLogger
from shared.misc.utilities import http_return

# Google API imports
from google.cloud import bigquery as gc_bigquery


def merge(tgt_ds_tbl: str, stg_ds_tbl: str, logger: GCPLogger):
    try:
        bq_client = gc_bigquery.Client()
        
        merge_stg_to_tgt_tbl_job = merge_stg_to_tgt_tbl(bq_client, tgt_ds_tbl, stg_ds_tbl)
        if merge_stg_to_tgt_tbl_job.error_result:
            msg = f"Error merging staging to target table: {merge_stg_to_tgt_tbl_job.error_result}"
            logger.error(msg)
            return http_return(500, msg)
    except Exception as e:
        msg = f"Error merging data from staging to target table in BigQuery"
        logger.error(msg)
        return http_return(500, msg)


def merge_stg_to_tgt_tbl(bq_client, tgt_ds_tbl, stg_ds_tbl):
    merge_tbls_query = \
        f"""
            MERGE INTO {tgt_ds_tbl} AS target
            USING      {stg_ds_tbl} AS staging
            ON   target.symbol    = staging.symbol 
            AND  target.market_dt = staging.market_dt
            WHEN MATCHED     THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """
    
    merge_tbls_query_job = bq_client.query(merge_tbls_query)
    merge_tbls_query_job.result()
    
    return merge_tbls_query_job

