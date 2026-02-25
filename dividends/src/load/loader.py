from google.cloud import bigquery as bq
from src.clients.gcp_services import MS_FILE_NM_WO_EXT, GCS_FILE_PATH, GCS_DIR_PATH


def load(data_cat, bucket_nm, bucket_dir_path, dataset_nm, batch_dt, start_dt, end_dt, logger, **kwargs):
    # For the load step, we will read the transformed data from GCS and load it into a BigQuery table. We will use the BigQuery client library to perform the load operation. We will also create the target table if it does not exist and we will use a staging table to perform the merge operation to ensure that we do not have duplicate records in the target table. We will partition the target table by market_dt and cluster by symbol and market_dt for optimized query performance.
    try:
        bq_client = bq.Client()
        match data_cat:
            case "dividends": create_tgt_tbl_job = create_dividends_tgt_tbl(bq_client, data_cat, dataset_nm)
            case _: print("Unknown data category")
        
        if create_tgt_tbl_job.error_result:  return {"status": "error", "message": f"Error creating target table: {create_tgt_tbl_job.error_result}"}
        
        # Create staging table if it does not exist
        create_stg_tbl_job = create_stg_tbl(bq_client, data_cat, dataset_nm, batch_dt)
        if create_stg_tbl_job.error_result:  return {"status": "error", "message": f"Error creating staging table: {create_stg_tbl_job.error_result}"}
        
        # Load the transformed data from GCS to the staging table in BigQuery
        file_nm = MS_FILE_NM_WO_EXT(data_cat, batch_dt, start_dt, end_dt)
        gcs_file_path = GCS_FILE_PATH(batch_dt, bucket_nm, bucket_dir_path, f"{file_nm}*.parquet")  # Load all parquet files for this batch into the staging table
        load_to_stg_tbl_job = load_to_stg_tbl(bq_client, data_cat, bucket_nm, bucket_dir_path, dataset_nm, f"staging_ms_{data_cat}_{batch_dt}", partition_col="market_dt", cluster_cols=["symbol", "market_dt"], save_mode="WRITE_TRUNCATE", batch_dt=batch_dt)
        if load_to_stg_tbl_job.errors:  return {"status": "error", "message": f"Error loading data to staging table: {load_to_stg_tbl_job.errors}"}
        
        merge_stg_to_tgt_tbl_job = merge_stg_to_tgt_tbl(bq_client, data_cat, dataset_nm, batch_dt)
        if merge_stg_to_tgt_tbl_job.error_result:  return {"status": "error", "message": f"Error merging staging to target table: {merge_stg_to_tgt_tbl_job.error_result}"}
    
    except Exception as e:
        return {"status": "error", "message": f"Error loading data to BigQuery: {e}"}
    
    return {"status": "success", "message": f"Loading to BigQuery is complete"}
    
def create_dividends_tgt_tbl(bq_client, data_cat, dataset_nm):
    create_tbl_query = \
        f"""
            CREATE TABLE IF NOT EXISTS {dataset_nm}.ms_{data_cat.lower()} (
                symbol STRING,
                market_dt DATE,
                dividend_ratio DOUBLE,
                distr_freq STRING,
                payment_dt DATE,
                record_dt DATE,
                declar_dt DATE
            )  
            PARTITIONED BY (market_dt)  CLUSTERED BY (symbol, market_dt)
        """
    
    create_tbl_query_job = bq_client.query(create_tbl_query)
    create_tbl_query_job.result()
    
    return create_tbl_query_job

def create_stg_tbl(bq_client, data_cat, dataset_nm, batch_dt):
    tgt_ds_tbl = f"{dataset_nm}.ms_{data_cat.lower()}"
    stg_ds_tbl = f"{dataset_nm}.staging_ms_{data_cat.lower()}_{batch_dt}"
    
    create_tbl_query = \
        f"""
            CREATE TABLE IF NOT EXISTS {tgt_ds_tbl} 
            LIKE {stg_ds_tbl}
        """
    
    create_tbl_query_job = bq_client.query(create_tbl_query)
    create_tbl_query_job.result()
    
    return create_tbl_query_job

def load_to_stg_tbl(bq_client, data_cat, bucket_nm, bucket_dir_nm, dataset_nm, partition_col, cluster_cols, save_mode, batch_dt):
    #
    stg_ds_tbl = f"{dataset_nm}.staging_ms_{data_cat.lower()}_{batch_dt}"
    disposition = "OVERWRITE" if save_mode.upper() == "WRITE_TRUNCATE" else "INTO"
    gcs_dir_path = GCS_DIR_PATH(batch_dt, bucket_nm, bucket_dir_nm)
    load_tbl_query = \
        f"""
            LOAD DATA {disposition} {stg_ds_tbl}
            PARTITION BY DATE({partition_col})
            CLUSTER BY {cluster_cols}
            FROM FILES (
            format = 'PARQUET',
            uris = ['{gcs_dir_path}/*']
            )
        """
    
    load_tbl_query_job = bq_client.query(load_tbl_query)
    load_tbl_query_job.result()
    
    return load_tbl_query_job

def merge_stg_to_tgt_tbl(bq_client, data_cat, dataset_nm, batch_dt):
    tgt_ds_tbl = f"{dataset_nm}.ms_{data_cat.lower()}"
    stg_ds_tbl = f"{dataset_nm}.staging_ms_{data_cat.lower()}_{batch_dt}"
    
    merge_tbls_query = \
        f"""
            MERGE INTO {tgt_ds_tbl} AS target
            USING {stg_ds_tbl} AS staging
            ON target.symbol = staging.symbol AND target.market_dt = staging.market_dt
            WHEN MATCHED THEN
                UPDATE SET *
            WHEN NOT MATCHED THEN
                INSERT *
        """
    
    merge_tbls_query_job = bq_client.query(merge_tbls_query)
    merge_tbls_query_job.result()
    
    return merge_tbls_query_job
