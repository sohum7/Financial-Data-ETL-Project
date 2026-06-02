# Run ETL Pipeline

# Built-in imports
from pandas import DataFrame as pd_DataFrame

# ETL imports
from etl.extract.extractor import extract as run_extract
from etl.transform.transformer import transform as run_transform
from etl.load.loader import load as run_load
from etl.load.merger import merge as run_merge

# Shared imports
from shared.clients.gcp.logging import CloudLogger
from shared.configs.config_loader import load_settings
from shared.misc.utilities import get_past_week_range, dict_to_logs


# Modifiable parameters for running the pipeline
def main():
    category = "dividends"
    full_refresh = False
    manual_override_dates = True
    
    kwargs = {} if not manual_override_dates else {
        "override_dates": {
            "batch_dt": "2026-01-31",
            "start_dt": "2024-01-01",
            "end_dt"  : "2024-12-31"
        }
    }
    
    run_pipeline(category, full_refresh, **kwargs)

# Run the ETL pipeline
def run_pipeline(data_type: str, full_refresh: bool = False, **kwargs):
    batch_dt: str
    start_dt: str
    end_dt: str
    
    # If override_dates is provided in kwargs, use those values for batch_dt, start_dt, and end_dt. 
    # Otherwise, use get_past_week_range() to get those values.
    # TODO: may only need to run this a few times a year rather than weekly
    batch_dt, start_dt, end_dt = get_past_week_range() if not kwargs.get("override_dates") \
    else (
            kwargs["override_dates"]["batch_dt"],
            kwargs["override_dates"]["start_dt"],
            kwargs["override_dates"]["end_dt"] 
        )
    
    # Get settings data
    settings = load_settings(data_type, start_dt, end_dt, batch_dt)
    
    ms_api_obj = settings.ms_api_obj
    raw_gcs_path_obj = settings.ms_raw_gcs_path
    tfd_gcs_path_obj = settings.ms_tfd_gcs_path
    tgt_bq_table_obj = settings.ms_tgt_bq_metadata
    stg_bq_table_obj = settings.ms_stg_bq_metadata
    
    
    #################################################    ETL PROCESS STARTING    #################################################
    with CloudLogger() as gcp_logger:
        raw_json: list[dict]
        extract_metrics: dict[str, str | int]
        
        tfd_df: pd_DataFrame
        transform_metrics: dict[str, int]
        
        
        gcp_logger.info(f"****************  ETL process for {data_type.upper()} starting  ****************")
        
        # EXTRACTION
        raw_json, extract_metrics = run_extract(ms_api_obj, raw_gcs_path_obj, full_refresh, gcp_logger)
        
        # TRANSFORMATION
        tfd_df, transform_metrics = run_transform(raw_json, tfd_gcs_path_obj, full_refresh, gcp_logger)
        
        # LOADING
        run_load(tfd_df, ms_api_obj.data_type, tgt_bq_table_obj, stg_bq_table_obj, gcp_logger)
        run_merge(ms_api_obj.data_type, tgt_bq_table_obj, stg_bq_table_obj, gcp_logger)
        
        gcp_logger.info(f"****************  ETL process for {data_type.upper()} completed  ****************")
        
        # Necessary metrics logging
        dict_to_logs(extract_metrics, gcp_logger)
        dict_to_logs(transform_metrics, gcp_logger)
        
    #################################################    ETL PROCESS SUCCEEDED    ################################################

# Run pipeline via main function
if __name__ == "__main__":
    main()