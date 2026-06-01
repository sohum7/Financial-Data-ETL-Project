# Load logic for dividends data category to BigQuery

# Builtin imports
from logging import Logger

# Shared imports
from shared.clients.gcp.bq import TableConfig
from shared.clients.gcp.logging import CloudLogger
from shared.clients.gcp.services import merge_table
from shared.configs.schema import get_columns, get_primary_columns


# Merging entry point
def merge(data_type: str, tgt_bq_table_obj: TableConfig, stg_bq_table_obj: TableConfig, logger: Logger | CloudLogger) -> None:
    """Merge staging/temp table into target table while ensuring duplicate records are not inserted

    Args:
        data_type (str): The type of data 
        tgt_bq_table_obj (TableConfig): Contains necessary Google BigQuery table metadata for the target table
        stg_bq_table_obj (TableConfig): Contains necessary Google BigQuery table metadata for the staging table
        logger (Logger | CloudLogger): Utilized for logging steps taken as well as errors

    Returns: None
    """
    
    tgt_ds_tbl = tgt_bq_table_obj.ds_tbl
    stg_ds_tbl = stg_bq_table_obj.ds_tbl
    
    # Get all column names and join columns later used for the merge query
    all_cols = [ row[0] for row in get_columns(data_type) ]
    join_cols = get_primary_columns(data_type)
    
    logger.info(f"RUNNING: Merging process from staging table {stg_ds_tbl} to target table {tgt_ds_tbl}...")
    
    # Merge data from staging to target table
    merge_main(tgt_bq_table_obj, stg_bq_table_obj, all_cols, join_cols, logger)
    
    logger.info(f"SUCCESS: Merging data from staging table {stg_ds_tbl} to target table {tgt_ds_tbl} completed.")

# Merge from staging table to target table
def merge_main(tgt_bq_table_obj: TableConfig, stg_bq_table_obj: TableConfig, all_cols: list[str], join_cols: list[str], logger: Logger | CloudLogger) -> None:
    """Merge staging/temp table into target table while ensuring duplicate records are not inserted

    Args:
        tgt_bq_table_obj (TableConfig): Contains necessary Google BigQuery table metadata for the target table
        stg_bq_table_obj (TableConfig): Contains necessary Google BigQuery table metadata for the staging table
        all_cols (list[str]): All columns for the table
        join_cols (list[str]): All join columns for the table to ensure duplicate records are not inserted
        logger (Logger | CloudLogger): Utilized for logging steps taken as well as errors

    Returns: None
    """
    
    logger.info("Running the main merge operation...")
    
    merge_table(tgt_bq_table_obj.dataset,
                tgt_bq_table_obj.table,
                stg_bq_table_obj.dataset,
                stg_bq_table_obj.table,
                all_cols,
                join_cols)
    
    logger.info("Main merge operation completed.")

