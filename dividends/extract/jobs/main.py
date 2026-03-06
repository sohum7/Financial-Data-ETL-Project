# Builtin imports
from json import dumps as json_dumps
from json import loads as json_loads

# Third-party imports
from shared.clients.gcp_logging import GCPLogger
from shared.clients.gcp_services import get_secret as gcp_get_secret
from shared.misc.utilities import getCurWkDtRange, http_return
from shared.configs.config_loader import MS_BASE_URL, MS_CAT, MS_SYMBOLS, MS_DIV_RAW_FILE_BUCKET_NM, MS_DIV_RAW_FILE_BUCKET_SUBDIR

# Local imports (added to path for cleaner imports)
from os import path as os_path
from sys import path as sys_path

current_dir = os_path.dirname(os_path.abspath(__file__))
sys_path.insert(0, os_path.join(current_dir, "src"))
sys_path.insert(0, os_path.join(current_dir, "shared"))

from extractor import extract_handler as run_extract



def extract(request):
    # Parse JSON body
    request_json = request.get_json(silent=True)
    if not request_json:
        return http_return(400, "Missing JSON body")
    
    # TODO: CHANGE BACK TO PARSING FROM REQUEST JSON ONCE TESTING IS DONE. THIS IS JUST FOR TESTING PURPOSES TO AVOID HAVING TO SEND A JSON BODY IN THE REQUEST EVERY TIME.
    #data_cat = request_json.get("data_category")
    data_cat = "dividends" # for testing
    
    if data_cat == "dividends":
        return extract_dividends(request)
    else:
        return http_return(400, f"Unsupported data category: {data_cat}")

def extract_dividends(_request):
    '''DELETE THIS ONCE TESTING IS DONE. THIS IS JUST FOR TESTING PURPOSES TO AVOID HAVING TO SEND A JSON BODY IN THE REQUEST EVERY TIME.'''
    '''TEST START - change yaml entry point?'''
    # _request is not being used in this function for now. we will parse the required parameters from the request once testing is done. for now, we are hardcoding the parameters for testing purposes to avoid having to send a JSON body in the request every time we want to test the function. we will change this back to parsing from the request JSON once testing is done.
    MS_V2_API_KEY = gcp_get_secret("MARKET_STACK_V2_API_KEY")
    # for testing (this will be within airflow later)
    todays_dt, past_monday_dt, past_friday_dt = getCurWkDtRange()
    request = { "data_category": MS_CAT, \
                "base_url": MS_BASE_URL, \
                "symbols": MS_SYMBOLS, \
                "api_key": MS_V2_API_KEY, \
                "bucket_name": MS_DIV_RAW_FILE_BUCKET_NM, \
                "bucket_directory_name": MS_DIV_RAW_FILE_BUCKET_SUBDIR, \
                "batch_date": todays_dt, \
                "start_date": past_monday_dt, \
                "end_date": past_friday_dt }
    '''TEST END'''
    
    # Required parameters
    data_cat = request.get("data_category")
    base_url = request.get("base_url")
    symbols_lst = request.get("symbols")
    api_key = request.get("api_key")
    bucket_nm = request.get("bucket_name")
    bucket_dir_nm= request.get("bucket_directory_name")
    batch_dt = request.get("batch_date")
    start_dt = request.get("start_date")
    end_dt = request.get("end_date")

    # Validate required fields
    missing = [p for p in ["data_category", "base_url", "symbols", "api_key", "bucket_name", "bucket_directory_name", "batch_date", "start_date", "end_date"] if not request.get(p)]
    if missing:
        return http_return(400, f"Missing required fields: {missing}")

    # Optional kwargs (future-proofing)
    optional_kwargs = request.get("options", {})
    with GCPLogger() as gcp_logger:
        # Call the pure extractor logic
        json_status_res = run_extract(data_cat, base_url, symbols_lst, api_key, bucket_nm, bucket_dir_nm, batch_dt, start_dt, end_dt, logger=gcp_logger, **optional_kwargs)
        pass
    return json_status_res
