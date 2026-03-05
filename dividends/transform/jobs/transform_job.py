from src.transformer import transform_handler as run_transform

from shared.clients.gcp_logging import GCPLogger
from shared.misc.utilities import http_return

def transform(request):
    # Parse JSON body
    request_json = request.get_json(silent=True)
    if not request_json:
        return http_return(400, "Missing JSON body")
    
    data_cat = request_json.get("data_category")
    if data_cat == "dividends":
        return transform_dividends(request)
    else:
        return http_return(400, f"Unsupported data category: {data_cat}")

def transform_dividends(request):
    # Required parameters
    data_cat = request.get("data_category")
    raw_bucket_nm = request.get("raw_bucket_name")
    raw_dir_nm = request.get("raw_directory_name")
    tfd_bucket_nm = request.get("transformed_bucket_name")
    tfd_dir_nm = request.get("transformed_directory_name")
    tfd_file_type = request.get("transformed_file_type", "parquet")  # Default to parquet if not provided
    tfd_save_mode = request.get("transformed_save_mode", "append")  # Default to append if not provided
    batch_dt = request.get("batch_date")
    start_dt = request.get("start_date")
    end_dt = request.get("end_date")

    # Validate required fields
    missing = [p for p in ["raw_bucket_name", "raw_directory_name", "transformed_bucket_name", "transformed_directory_name", "transformed_file_type", "transformed_save_mode", "batch_date", "start_date", "end_date"] if not request.get(p)]
    if missing:
        return http_return(400, f"Missing required fields: {missing}")

    # Optional kwargs (future-proofing)
    optional_kwargs = request.get("options", {})
    
    # Call the pure transformer logic
    json_status_res = run_transform(data_cat, raw_bucket_nm, raw_dir_nm, tfd_bucket_nm, tfd_dir_nm, batch_dt, start_dt, end_dt, tfd_file_type=tfd_file_type, tfd_save_mode=tfd_save_mode, **optional_kwargs)
    return json_status_res