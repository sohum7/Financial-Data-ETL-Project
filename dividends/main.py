from src.extract.extractor import extract_generic as extract_generic_main
from src.transform.transformer import transform_dividends as transform_dividends_main
from src.load.loader import load as load_main
from src.clients.gcp_logging import GCPLogger
from src.utilities import getCurWkDtRange, http_return
from json import dumps as json_dumps
from json import loads as json_loads



# for references
# bucket - market-stack-....-dev
# dir    - bronze/dividends/
# file   - DIVIDENDS_20240101_20240107.json
# blob   - {dir}{file} - bronze/dividends/DIVIDENDS_20240101_20240107.json
# extract
# input - start_dt, end_dt, data_catgy, symbols, api_key, url, bucket_nm, bucket_dir
# output - file path in gcs bucket where extracted data is stored
# isnt base url the same?
# 1 weeks worth of data for all symbols goes into 1 file in gcs bucket. file name is based on data category and date range of data.
# extracting for dividends is similar to tickers, etc, but transforming is different based on the structure of data returned by marketstack api. so we can have a generic extract function but separate transform functions for each data category. or we can have separate extract and transform functions for each data category. we will go with the former approach to avoid code duplication in extract functions and have more modular code.

# transform
# input - start_dt, end_dt, data_catgy, symbols, raw_bucket_nm, raw_bucket_dir_nm, tfd_bucket_nm, tfd_bucket_dir_nm
# output - multiple files in gcs bucket where transformed data is stored. file format is delta lake. file name is based on data category and date range of data.


# TODO:
# HANDLE EXCEPTIONS IN THE LOGIC FUNCTIONS

def extract_dividends(_request):
    # _request not needed for now but keeping it for future-proofing and consistency with transform and load functions. we can remove it later if we decide it's not needed.
    
    # for testing (this will be within airflow later)
    todays_dt, past_monday_dt, past_friday_dt = getCurWkDtRange()
    request = { "data_category": "dividends", \
                "base_url": "http://api.marketstack.com/v1/dividends", \
                "symbols": ["AAPL", "MSFT", "GOOGL"], \
                "api_key": "your_api_key", \
                "bucket_name": "your_bucket_name", \
                "bucket_directory_name": "bronze/dividends/", \
                "batch_date": todays_dt, \
                "start_date": past_monday_dt, \
                "end_date": past_friday_dt}
    return extract(json_dumps(request))

def extract(request):
    # Parse JSON body
    request_json = json_loads(request) if isinstance(request, str) else request.get_json(silent=True)
    
    if not request_json:
        return http_return(400, "Missing JSON body")
    
    # Required parameters
    data_cat = request_json.get("data_category")
    base_url = request_json.get("base_url")
    symbols_lst = request_json.get("symbols")
    api_key = request_json.get("api_key")
    bucket_nm = request_json.get("bucket_name")
    bucket_dir_nm= request_json.get("bucket_directory_name")
    batch_dt = request_json.get("batch_date")
    start_dt = request_json.get("start_date")
    end_dt = request_json.get("end_date")

    # Validate required fields
    missing = [p for p in ["data_category", "base_url", "symbols", "api_key", "bucket_name", "bucket_directory_name", "batch_date", "start_date", "end_date"] if not request_json.get(p)]
    if missing:
        return http_return(400, f"Missing required fields: {missing}")

    # Optional kwargs (future-proofing)
    optional_kwargs = request_json.get("options", {})
    
    with GCPLogger() as gcp_logger:
        # Call the pure extractor logic
        json_status_res = extract_generic_main(data_cat, base_url, symbols_lst, api_key, bucket_nm, bucket_dir_nm, batch_dt, start_dt, end_dt, logger=gcp_logger, **optional_kwargs)
    return json_status_res

def transform(request):
    # Parse JSON body
    request_json = request.get_json(silent=True)
    if not request_json:
        return http_return(400, "Missing JSON body")
    
    if request_json.get("data_category") == "dividends":
        return transform_dividends(request)
    else:
        return http_return(400, f"Unsupported data category: {request_json.get('data_category')}")

def transform_dividends(request_json):
    data_cat = "dividends"

    # Required parameters
    data_cat = request_json.get("data_category")
    raw_bucket_nm = request_json.get("raw_bucket_name")
    raw_dir_nm = request_json.get("raw_directory_name")
    tfd_bucket_nm = request_json.get("transformed_bucket_name")
    tfd_dir_nm = request_json.get("transformed_directory_name")
    tfd_file_type = request_json.get("transformed_file_type", "parquet")  # Default to parquet if not provided
    tfd_save_mode = request_json.get("transformed_save_mode", "append")  # Default to append if not provided
    batch_dt = request_json.get("batch_date")
    start_dt = request_json.get("start_date")
    end_dt = request_json.get("end_date")

    # Validate required fields
    missing = [p for p in ["raw_bucket_name", "raw_directory_name", "transformed_bucket_name", "transformed_directory_name", "transformed_file_type", "transformed_save_mode", "batch_date", "start_date", "end_date"] if not request_json.get(p)]
    if missing:
        return http_return(400, f"Missing required fields: {missing}")

    # Optional kwargs (future-proofing)
    optional_kwargs = request_json.get("options", {})
    
    # Call the pure transformer logic
    json_status_res = transform_dividends_main(data_cat, raw_bucket_nm, raw_dir_nm, tfd_bucket_nm, tfd_dir_nm, batch_dt, start_dt, end_dt, tfd_file_type=tfd_file_type, tfd_save_mode=tfd_save_mode, **optional_kwargs)
    return json_status_res


def load(request):
    # Parse JSON body
    request_json = request.get_json(silent=True)
    if not request_json:
        return http_return(400, "Missing JSON body")
    
    # Required parameters
    data_cat = request_json.get("data_category")
    bucket_nm = request_json.get("bucket_name")
    bucket_dir_nm = request_json.get("bucket_directory_name")
    dataset_nm = request_json.get("dataset_name")
    batch_dt = request_json.get("batch_date")
    start_dt = request_json.get("start_date")
    end_dt = request_json.get("end_date")
    
    # Validate required fields
    missing = [p for p in ["data_category", "bucket_name", "bucket_directory_name", "dataset_name", "batch_date", "start_date", "end_date"] if not request_json.get(p)]
    if missing:
        return http_return(400, f"Missing required fields: {missing}")
    
    # Optional kwargs (future-proofing)
    optional_kwargs = request_json.get("options", {})
    
    # Call the pure load logic
    json_status_res = load_main(data_cat, bucket_nm, bucket_dir_nm, dataset_nm, batch_dt, start_dt, end_dt, **optional_kwargs)
    return json_status_res

