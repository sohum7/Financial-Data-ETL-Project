from google.cloud import bigquery as bq
from src.clients.gcp_services import MS_FILE_NM_WO_EXT, GCS_FILE_PATH


def load(data_cat, bucket_nm, bucket_dir_nm, dataset_nm, batch_dt, start_dt, end_dt, **kwargs):
    # For the load step, we will read the transformed data from GCS and load it into a BigQuery table. We will use the BigQuery client library to perform the load operation. We will also create the target table if it does not exist and we will use a staging table to perform the merge operation to ensure that we do not have duplicate records in the target table. We will partition the target table by market_dt and cluster by symbol and market_dt for optimized query performance.
    try:
        bq_client = bq.Client()
        if data_cat == "dividends":
            query_create_target_tbl = create_dividends_tgt_tbl(data_cat, dataset_nm)
        else:
            pass
        
        # Create target table if it does not exist
        create_tgt_tbl_res = bq_client.query(query_create_target_tbl).result()  # Create target table if it does not exist
        if create_tgt_tbl_res.errors:  return {"status": "error", "message": f"Error creating target table: {create_tgt_tbl_res.errors}"}, 500
        
        # Create staging table if it does not exist
        query_create_staging_tbl = create_stg_tbl(data_cat, dataset_nm, batch_dt)
        create_stg_tbl_res = bq_client.query(query_create_staging_tbl).result()  # Create staging table for this batch
        if create_stg_tbl_res.errors:  return {"status": "error", "message": f"Error creating staging table: {create_stg_tbl_res.errors}"}, 500
        
        # Load the transformed data from GCS to the staging table in BigQuery
        file_nm = MS_FILE_NM_WO_EXT(data_cat, batch_dt, start_dt, end_dt)
        gcs_file_path = GCS_FILE_PATH(batch_dt, bucket_nm, bucket_dir_nm, f"{file_nm}*.parquet")  # Load all parquet files for this batch into the staging table
        load_to_stg_tbl_res = load_to_stg_tbl(bq_client, gcs_file_path, dataset_nm, f"staging_ms_{data_cat}_{batch_dt}", partition_col="market_dt", cluster_cols=["symbol", "market_dt"], save_mode="WRITE_APPEND")
        if load_to_stg_tbl_res.errors:  return {"status": "error", "message": f"Error loading data to staging table: {load_to_stg_tbl_res.errors}"}, 500
        
        query_merge_stg_to_tgt_tbl = merge_stg_to_tgt_tbl(data_cat, dataset_nm, batch_dt)
        merge_stg_to_tgt_tbl_res = bq_client.query(query_merge_stg_to_tgt_tbl).result()  # Merge staging table data into target table
        if merge_stg_to_tgt_tbl_res.errors:  return {"status": "error", "message": f"Error merging staging to target table: {merge_stg_to_tgt_tbl_res.errors}"}, 500
    
    except Exception as e:
        return {"status": "error", "message": f"Error loading data to BigQuery: {e}"}, 500
    
def create_dividends_tgt_tbl(data_cat, dataset_nm):
    return f"""
                CREATE TABLE IF NOT EXISTS {dataset_nm}.ms_{data_cat.lower()} (
                    symbol STRING,
                    market_dt DATE,
                    dividend_ratio DOUBLE,
                    distr_freq STRING,
                    payment_dt DATE,
                    record_dt DATE,
                    declar_dt DATE
                )  PARTITIONED BY (market_dt)  CLUSTERED BY (symbol, market_dt)
            """

def create_stg_tbl(data_cat, dataset_nm, batch_dt):
    return f"""
                CREATE TABLE IF NOT EXISTS {dataset_nm}.staging_ms_{data_cat}_{batch_dt} 
                LIKE {dataset_nm}.ms_{data_cat.lower()}
            """

def load_to_stg_tbl(bq_client, gcs_file_path, dataset_nm, tbl_nm, partition_col, cluster_cols, save_mode):
    job_config = bq.LoadJobConfig(
        source_format=bq.SourceFormat.PARQUET,
        write_disposition=save_mode.upper(),  # "WRITE_TRUNCATE", "WRITE_APPEND", or "WRITE_EMPTY"
        time_partitioning=bq.TimePartitioning(
            type_=bq.TimePartitioningType.DAY,
            field=partition_col
        ),
        clustering_fields=cluster_cols
    )
    
    ds_tbl_nm = f"{dataset_nm}.{tbl_nm}"
    load_job = bq_client.load_table_from_uri(gcs_file_path, ds_tbl_nm, job_config=job_config)
    return load_job.result()  # Wait for the job to complete
    


def merge_stg_to_tgt_tbl(data_cat, dataset_nm, batch_dt):
    return f"""
                MERGE INTO {dataset_nm}.ms_{data_cat.lower()} AS target
                USING {dataset_nm}.staging_ms_{data_cat}_{batch_dt} AS staging
                ON target.symbol = staging.symbol AND target.market_dt = staging.market_dt
                WHEN MATCHED THEN
                    UPDATE SET *
                WHEN NOT MATCHED THEN
                    INSERT *
            """
