#

# Builtin imports
from typing import Sequence, Hashable
# Shared imports
from google.cloud import bigquery as gc_bigquery
from shared.misc.utilities import http_return


def merge(tgt_ds_tbl, stg_ds_tbl, logger, **kwargs):
    try:
        bq_client = gc_bigquery.Client()
        
        create_tgt_tbl_job = create_dividends_tgt_tbl(bq_client, tgt_ds_tbl)
        if create_tgt_tbl_job.error_result:
            msg = f"Error creating target table: {create_tgt_tbl_job.error_result}"
            logger.error(msg)
            return http_return(500, msg)

        merge_stg_to_tgt_tbl_job = merge_stg_to_tgt_tbl(bq_client, tgt_ds_tbl, stg_ds_tbl)
        if merge_stg_to_tgt_tbl_job.error_result:
            msg = f"Error merging staging to target table: {merge_stg_to_tgt_tbl_job.error_result}"
            logger.error(msg)
            return http_return(500, msg)
    except Exception as e:
        msg = f"Error merging data from staging to target table in BigQuery"
        logger.error(msg)
        return http_return(500, msg)

def create_dividends_tgt_tbl(bq_client: gc_bigquery.Client, ds_tbl: str, partition_col: str | None = "", *cluster_cols: str):
    optional_clause = ""
    if partition_col is not None:
        partition_col.strip()
        optional_clause = f"PARTITIONED BY {partition_col.strip()}"
    if not cluster_cols:
        cluster_cols_str = ", ".join( col.strip() for col in cluster_cols )
        optional_clause += f"CLUSTER BY ({cluster_cols_str})"
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

