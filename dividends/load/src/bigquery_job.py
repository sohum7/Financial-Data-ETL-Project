# BigQuery load job for various data categories

# Shared imports
from shared.misc.utilities import http_return

# Local imports
from load.src.loader import load_handler as run_load

def load(request):
    # Parse JSON body
    request_json = request.get_json(silent=True)
    if not request_json:
        return http_return(400, "Missing JSON body")
    
    data_cat = request_json.get("data_category")
    if data_cat == "dividends":
        return load_dividends(request)
    else:
        return http_return(400, f"Unsupported data category: {data_cat}")

def load_dividends(request):
    # Required parameters
    data_cat = request.get("data_category")
    bucket_nm = request.get("bucket_name")
    bucket_dir_nm = request.get("bucket_directory_name")
    dataset_nm = request.get("dataset_name")
    batch_dt = request.get("batch_date")
    start_dt = request.get("start_date")
    end_dt = request.get("end_date")
    
    # Validate required fields
    missing = [p for p in ["data_category", "bucket_name", "bucket_directory_name", "dataset_name", "batch_date", "start_date", "end_date"] if not request.get(p)]
    if missing:
        return http_return(400, f"Missing required fields: {missing}")
    
    # Optional kwargs (future-proofing)
    optional_kwargs = request.get("options", {})
    
    # Call the pure load logic
    json_status_res = run_load(data_cat, bucket_nm, bucket_dir_nm, dataset_nm, batch_dt, start_dt, end_dt, **optional_kwargs)
    return json_status_res

