from requests import get as requests_get
from requests.exceptions import HTTPError, RequestException
from datetime import datetime
from json import JSONDecodeError

from shared.clients.gcp_logging import GCPLogger
from shared.clients.gcp_services import write_json_to_gcs
from shared.misc.utilities import http_return

def extract_handler(data_cat: str, base_url: str, symbols_lst: list, api_key: str, bucket_nm: str, bucket_dir_path: str, batch_dt: datetime | str, start_dt: datetime | str, end_dt: datetime | str, logger: GCPLogger, **kwargs):
    max_req_rows = kwargs.get("min_rows", 5*len(symbols_lst))  # Default to 5 rows per symbol if not provided
    req_limit = kwargs.get("limit", max_req_rows)
    sort_type = kwargs.get("sort", "ASC")
    
    if isinstance(start_dt, datetime):
        start_dt = start_dt.strftime("%Y-%m-%d")
    if isinstance(end_dt, datetime):
        end_dt = end_dt.strftime("%Y-%m-%d")
    
    if start_dt > end_dt:
        msg = f"Invalid date range: start_date {start_dt} is after end_date {end_dt}"
        logger.error(msg)
        return http_return(400, msg)
    
    # company symbols from which to extract data from
    symbols_params_str = ",".join(symbols_lst)
    
    # API request parameters for Marketstack
    req_params = {
        "access_key": api_key,
        "symbols": symbols_params_str,
        "limit": req_limit,
        "date_from": start_dt,
        "date_to": end_dt,
        "sort": sort_type
    }
    
    # Construct the full API URL for the given data category
    full_url = f"""{base_url.strip('"')}/{data_cat.strip('"')}"""
    logger.info(f"Constructed API URL: {full_url} with params: {req_params}")
    
    try:
        # Make the API request and get the JSON response
        res_json = extract_generic_main(full_url, req_params)
        
        # Write the JSON data to GCS
        file_path = write_json_to_gcs(data_cat, res_json, bucket_nm, bucket_dir_path, batch_dt, start_dt, end_dt)
    # Exception Handling
    except HTTPError as e:
        msg = f"HTTP error occurred: {e}"
        logger.error(msg)
        return http_return(500, msg)
    except RequestException as e:
        msg = f"API request failed: {e}"
        logger.error(msg)
        return http_return(500, msg)
    except JSONDecodeError as e:
        msg = f"Error parsing API response to JSON: {e}"
        logger.error(msg)
        return http_return(500, msg)
    except Exception as e:
        msg = f"An unexpected error occurred: {e}"
        logger.error(msg)
        return http_return(500, msg)
    
    # Extraction succeeded
    msg = f"Data extracted and written to GCS: {file_path}"
    logger.error(msg)
    return http_return(200, msg)
    
def extract_generic_main(full_url, req_params):
    # make the API request
    res = requests_get(full_url, params=req_params)
    res.raise_for_status()  # Raise an error for HTTP errors
    
    # Parse the JSON response
    res_json = res.json()
    
    return res_json