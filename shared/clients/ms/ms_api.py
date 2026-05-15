# Main extractor logic for various data categories

# Built-in imports
from dataclasses import dataclass
from datetime import datetime
from json import JSONDecodeError
from requests import get as requests_get
from requests.exceptions import HTTPError, RequestException

# Shared Imports
#from shared.clients.gcp.logging import GCPLogger


@dataclass
class MSAPILib:
    data_cat: str
    base_url: str
    symbols_lst_str: str
    api_key: str
    batch_dt: str
    start_dt: str
    end_dt: str
    
    def __post_init__(self):
        self.data_cat = self.data_cat.lower()
        self.base_url = self.base_url.rstrip("/")
        
        #something for batch start and end dates (like conversions and validations)
    
    def url_without_params(self):
        return f"{self.base_url}/{self.data_cat}"



def ms_api_request(data_cat: str, base_url: str, symbols_lst_str: str, api_key: str, batch_dt: str, start_dt: str, end_dt: str, logger, **kwargs):
    # Chck if valid url    
    max_req_rows = kwargs.get("min_rows", 5*len(symbols_lst_str))  # Default to 5 rows per symbol if not provided
    req_limit = kwargs.get("limit", max_req_rows)
    sort_type = kwargs.get("sort", "ASC")
    
    # API request parameters for Marketstack
    req_params = {
        "access_key": api_key,
        "symbols": symbols_lst_str,
        "limit": req_limit,
        "date_from": start_dt,
        "date_to": end_dt,
        "sort": sort_type
    }
    
    full_url = ms_url_constructor(base_url, data_cat)
    logger.info(f"Constructed API URL: {full_url} w/ params: {req_params}")
    
    msg = ""
    try:
        # make the API request
        resp = requests_get(full_url, params=req_params)
        resp.raise_for_status()  # Raise an error for HTTP errors
        
        # Parse the JSON response
        resp_json = resp.json()
    # Exception Handling
    except HTTPError as e:
        msg = f"FAILED: API extraction - HTTP error occurred: {e}"
    except RequestException as e:
        msg = f"FAILED: API extraction - request failed: {e}"
    except JSONDecodeError as e:
        msg = f"FAILED: API extraction - unable to parse API response to JSON: {e}"
    except Exception as e:
        msg = f"FAILED: API extraction - an unexpected error occurred: {e}"
    else:
        # Extraction from Marketstack API succeeded
        msg = f"SUCCESS: Data extracted from Marketstack API and JSON decoded successfully from....\n{full_url}\nbatch date: {batch_dt}\nstart date: {start_dt}\nend date: {end_dt}"
        logger.info(msg)
        return resp_json
    
    # Log the error message and return None to indicate failure
    logger.error(msg)
    return None
    

def ms_url_constructor(base_url, endpoint):
    if not base_url.endswith("/"):
        base_url += "/"
    return f"{base_url}{endpoint}"