import requests as req
from requests.exceptions import HTTPError, RequestException
from json import JSONDecodeError
from src.clients.gcp_services import write_json_to_gcs
from src.utilities import http_return

def extract_generic(data_cat, base_url, symbols_lst, api_key, bucket_nm, bucket_dir_path, batch_dt, start_dt, end_dt, logger, **kwargs):
    max_req_rows = kwargs.get("min_rows", 5*len(symbols_lst))  # Default to 5 rows per symbol if not provided
    req_limit = kwargs.get("limit", max_req_rows)
    sort_type = kwargs.get("sort", "ASC")
    
    # company symbols from which to extract data from
    symbols_params_str = ",".join(symbols_lst)
    
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