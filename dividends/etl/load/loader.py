# Main load logic for various data categories to BigQuery

# Built-in imports
from logging import Logger
from pandas import DataFrame as pd_DataFrame

# Shared imports
from shared.clients.gcp.logging import CloudLogger
from shared.clients.gcp.bq import TableConfig
from shared.clients.gcp.services import create_target_table, create_staging_table, load_table
from shared.configs.schema import get_columns


# Loading entry point
def load(df: pd_DataFrame, data_type: str, tgt_bq_table_obj: TableConfig, stg_bq_table_obj: TableConfig, logger: Logger | CloudLogger) -> None:
    """Loads transformed relational data into a staging/temp table while ensuring target and staging tables are created if not already

    Args:
        df (pd_DataFrame): Transformed Pandas DataFrame
        data_type: (str): Category of data (dividends, tickers, etc)
        tgt_bq_table_obj (TableConfig): Contains necessary Google BigQuery table metadata for the target table
        stg_bq_table_obj (TableConfig): Contains necessary Google BigQuery table metadata for the staging table
        logger (Logger | CloudLogger): Utilized for logging steps taken as well as errors
    
    Returns: None
    """
    
    # Retrieve column metadata (type, nullable, etc)
    col_metadata: list[tuple[str, ...]] = get_columns(data_type)
    
    logger.info(f"RUNNING: Starting load process into BigQuery staging table {stg_bq_table_obj.ds_tbl}...")
    
    # Create dividends target table if it doesn't exist
    create_dividends_tgt_tbl(tgt_bq_table_obj, col_metadata, logger)
    
    # Create or replace dividends staging table
    create_dividends_stg_tbl(tgt_bq_table_obj, stg_bq_table_obj, logger)
    
    # Load data to dividends staging table using the provided load function (either from df or uri)
    load_main(df, stg_bq_table_obj, logger)
    
    logger.info("SUCCESS: Load process to staging table completed.")

# Load from pandas DataFrame to staging table
def load_main(df: pd_DataFrame, stg_bq_table_obj: TableConfig, logger: Logger | CloudLogger) -> None:
    """Loads transformed relational data into a staging/temp table

    Args:
        df (pd_DataFrame): Transformed Pandas DataFrame
        stg_bq_table_obj (TableConfig): Contains necessary Google BigQuery table metadata for the staging table
        logger (Logger | CloudLogger): Utilized for logging steps taken as well as errors

    Returns: None
    """
    
    logger.info("Starting main load operation...")
    
    load_table(df,
                stg_bq_table_obj.dataset,
                stg_bq_table_obj.table,
                "WRITE_TRUNCATE")
    
    logger.info("Main load operation completed.")

# Create the target table for dividends category
def create_dividends_tgt_tbl(tgt_bq_table_obj: TableConfig, col_metadata: list[tuple[str, ...]], logger: Logger | CloudLogger) -> None:
    """Creates the dividend category's target/main table
    
    Args:
        tgt_bq_table_obj (TableConfig): Contains necessary Google BigQuery table metadata for the target table
        col_metadata (list[tuple[str, ...]]): Column data with associated constraints
        logger (Logger | CloudLogger): Utilized for logging steps taken as well as errors
    
    Returns: None
    """
    
    logger.info("Starting create target table operation...")
    
    create_target_table(tgt_bq_table_obj.dataset, 
                        tgt_bq_table_obj.table, 
                        col_metadata, 
                        tgt_bq_table_obj.partition_col.column if tgt_bq_table_obj.partition_col is not None else None, 
                        tgt_bq_table_obj.partition_col.granularity if tgt_bq_table_obj.partition_col is not None else None,
                        tgt_bq_table_obj.cluster_cols)
    
    logger.info("Create target table operation completed.")

# Create the staging table for dividends category
def create_dividends_stg_tbl(tgt_bq_table_obj: TableConfig, stg_bq_table_obj: TableConfig, logger: Logger | CloudLogger) -> None:
    """Creates the dividend category's staging/temp table
    
    Args:
        tgt_bq_table_obj (TableConfig): Contains necessary Google BigQuery table metadata for the target table
        stg_bq_table_obj (TableConfig): Contains necessary Google BigQuery table metadata for the staging table
        logger (Logger | CloudLogger): Utilized for logging steps taken as well as errors
    
    Returns: None
    """
    
    logger.info("Starting create staging table operation...")
    
    create_staging_table(tgt_bq_table_obj.dataset,
                        tgt_bq_table_obj.table,
                        stg_bq_table_obj.dataset,
                        stg_bq_table_obj.table)
    
    logger.info("Create staging table operation completed.")