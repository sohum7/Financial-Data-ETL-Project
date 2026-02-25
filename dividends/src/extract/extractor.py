import requests as req
from requests.exceptions import HTTPError, RequestException
from json import JSONDecodeError
from src.clients.gcp_services import write_json_to_gcs

def extract_generic(data_cat, base_url, symbols, api_key, bucket_nm, bucket_dir_path, batch_dt, start_dt, end_dt, logger, **kwargs):
    max_req_rows = kwargs.get("min_rows", 5*len(symbols))
    req_limit = kwargs.get("limit", max_req_rows)
    sort_type = kwargs.get("sort", "ASC")
    
    # company symbols from which to extract data from
    symbols_params_str = ",".join(symbols)
    
    # API request to Marketstack
    req_params = {
        "access_key": api_key,
        "symbols": symbols_params_str,
        "limit": req_limit,
        "date_from": start_dt.strftime("%Y-%m-%d"),
        "date_to": end_dt.strftime("%Y-%m-%d"),
        "sort": sort_type
    }
    
    # construct the full URL for the API request
    full_url = f"{base_url}/{data_cat}"
    
    try:
        # make the API request
        res = req.get(full_url, params=req_params)
        res.raise_for_status()  # Raise an error for HTTP errors
        
        # Parse the JSON response
        res_json = res.json()
        
        # Write the JSON data to GCS
        file_path = write_json_to_gcs(data_cat, res_json, bucket_nm, bucket_dir_path, batch_dt, start_dt, end_dt)
    
    # Exception Handling
    except HTTPError as e:
        logger.error(f"HTTP error occurred: {e}"); return False
    except RequestException as e:
        logger.error(f"API request failed: {e}"); return False
    except JSONDecodeError as e:
        logger.error(f"Error parsing API response to JSON: {e}"); return False
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}"); return False
    
    # Extraction succeeded
    logger.error(f"Data extracted and written to GCS: {file_path}")
    return True