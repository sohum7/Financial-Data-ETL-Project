# Market Stack api handling

# Built-in imports
from requests import get as requests_get

# Shared imports
from shared.clients.ms.api import APIConfig

# Main Market Stack API extractor logic
def ms_api_request(data_type: str, base_url: str, symbols_lst_str: str, api_key: str, start_dt: str, end_dt: str, **kwargs) -> dict:
    default_limit = 10000
    default_sort = "ASC"
    
    # Check if valid url
    req_limit = kwargs.get("limit", default_limit)
    sort_type = kwargs.get("sort", default_sort)
    
    # API request parameters for Marketstack
    req_params: dict[str, str] = {
        "access_key": api_key,
        "symbols": symbols_lst_str,
        "limit": req_limit,
        "date_from": start_dt,
        "date_to": end_dt,
        "sort": sort_type
    }
    
    # Construct the full API url
    full_url = APIConfig.url_constructor(base_url, data_type)
    
    # Submit the API request
    resp = requests_get(full_url, params=req_params)
    resp.raise_for_status()  # Raise an error for HTTP errors
    
    # Decode JSON to Python dictionary
    return resp.json()
