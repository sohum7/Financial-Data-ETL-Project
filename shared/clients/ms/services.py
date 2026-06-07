# Market Stack api handling

# Built-in imports
from requests import get as requests_get

# Shared imports
from shared.clients.ms.api import APIConfig

# Main Market Stack API extractor logic
def ms_api_request(data_type: str, base_url: str, symbols_lst_str: str, api_key: str, start_dt: str, end_dt: str, **kwargs) -> dict:
    """Submit a request to the Marketstack API and return the parsed JSON response. This function builds the request parameters, sends the HTTP request, and validates the response status.

    The function focuses on configuring query parameters such as symbols, date range, and sorting, while delegating URL construction to the API configuration. It then performs the HTTP GET call and returns the decoded JSON payload as a Python dictionary.

    Args:
        data_type: The type of Marketstack data to request (for example, 'dividends' or 'tickers').
        base_url: The base URL of the Marketstack API.
        symbols_lst_str: A comma-separated string of ticker symbols to request.
        api_key: The API access key for authenticating with Marketstack.
        start_dt: The start date for the data query in ISO or API-accepted string format.
        end_dt: The end date for the data query in ISO or API-accepted string format.
        **kwargs: Optional query parameters such as 'limit' and 'sort' to refine the request.

    Returns:
        A dictionary representing the JSON-decoded response body from the Marketstack API.
    """
    
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
