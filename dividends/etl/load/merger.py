# 

# Shared imports
from shared.clients.gcp.logging import GCPLogger
from shared.misc.utilities import http_return

# Google API imports
from google.api_core.exceptions import Conflict
from google.cloud import bigquery as gc_bigquery



def merge(tgt_ds_tbl: str, stg_ds_tbl: str, logger: GCPLogger) -> None:
    # Initialize BigQuery client
    bq_client = gc_bigquery.Client()
    
    # Merge data from staging to target table
    merge_stg_to_tgt_tbl(bq_client, tgt_ds_tbl, stg_ds_tbl, logger)

def merge_stg_to_tgt_tbl(bq_client: gc_bigquery.Client, tgt_ds_tbl: str, stg_ds_tbl: str, logger: GCPLogger) -> gc_bigquery.QueryJob:
    # Merge staging table to target tablequery
    merge_tbls_query = \
        f"""
            MERGE INTO {tgt_ds_tbl} AS target
            USING {stg_ds_tbl} AS staging
            ON target.symbol = staging.symbol
            AND target.market_dt = staging.market_dt
            WHEN MATCHED THEN
            UPDATE SET
                target.distr_freq = staging.distr_freq,
                target.payment_dt = staging.payment_dt,
                target.record_dt  = staging.record_dt,
                target.declar_dt  = staging.declar_dt,
                target.dividend_ratio   = staging.dividend_ratio
            WHEN NOT MATCHED THEN
            INSERT (symbol, market_dt, payment_dt, record_dt, declar_dt, dividend_ratio)
            VALUES (staging.symbol, staging.market_dt, staging.payment_dt, staging.record_dt, staging.declar_dt, staging.dividend_ratio);
        """
    
    # Execute the merge query and wait for it to complete
    merge_tbls_query_job = bq_client.query(merge_tbls_query)
    merge_tbls_query_job.result()
    
    # Check for errors in the merge query job and raise an exception if any are found
    if merge_tbls_query_job.errors:
        err_msg = f"Error merging staging to target table: {merge_tbls_query_job.error_result}"
        logger.error(err_msg)
        raise Conflict(err_msg)
    
    return merge_tbls_query_job

