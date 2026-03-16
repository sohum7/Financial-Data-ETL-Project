# 

# Shared imports
from shared.clients.gcp.logging import GCPLogger
from shared.misc.utilities import http_return

# Google API imports
from google.cloud import bigquery as gc_bigquery


def merge(tgt_ds_tbl: str, stg_ds_tbl: str, logger: GCPLogger) -> bool:
    try:
        bq_client = gc_bigquery.Client()
        
        merge_stg_to_tgt_tbl_job = merge_stg_to_tgt_tbl(bq_client, tgt_ds_tbl, stg_ds_tbl)
        if merge_stg_to_tgt_tbl_job.errors:
            msg = f"Error merging staging to target table: {merge_stg_to_tgt_tbl_job.error_result}"
            logger.error(msg)
    except Exception as e:
        msg = f"Error merging data from staging to target table in BigQuery"
        logger.error(msg)
    else: 
        return True
    
    return False


def merge_stg_to_tgt_tbl(bq_client: gc_bigquery.Client, tgt_ds_tbl: str, stg_ds_tbl: str) -> gc_bigquery.QueryJob:
    merge_tbls_query = \
        f"""
            MERGE INTO market_stack.dividends AS target
            USING market_stack_stg.dividends_stg AS staging
            ON target.symbol = staging.symbol
            AND target.market_dt = staging.market_dt
            WHEN MATCHED THEN
            UPDATE SET
                target.payment_dt = staging.payment_dt,
                target.record_dt  = staging.record_dt,
                target.declar_dt  = staging.declar_dt,
                target.dividend   = staging.dividend
            WHEN NOT MATCHED THEN
            INSERT (symbol, market_dt, payment_dt, record_dt, declar_dt, dividend)
            VALUES (staging.symbol, staging.market_dt, staging.payment_dt, staging.record_dt, staging.declar_dt, staging.dividend);
        """
    
    merge_tbls_query_job = bq_client.query(merge_tbls_query)
    merge_tbls_query_job.result()
    
    return merge_tbls_query_job

