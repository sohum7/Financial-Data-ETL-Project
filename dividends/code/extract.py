import requests as req
import json
from google.cloud import storage
from .utilities import getCurWkDtRange
from ..configs.config import MS_FILE_FORMAT, MS_DIVIDENDS, MS_DIVIDENDS_API_KEY, MS_DIVIDENDS_URL, MS_DIVIDENDS_RAW_FILE_TYPE, MS_DIVIDENDS_RAW_FILE_BUCKET, MS_DIVIDENDS_RAW_FILE_BUCKET_SUBDIR


def main():
    # company symbols from which to extract data from
    symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "FB", "TSLA", "NVDA"]
    symbols_params_str = ",".join(symbols)
    
    monday, friday = getCurWkDtRange()
    print(f'Symbols: {symbols_params_str}\nStart range: {monday}\nEnd range: {friday}')
    
    # API request to Marketstack for dividend data
    params = {
        "access_key": MS_DIVIDENDS_API_KEY,
        "symbols": symbols_params_str,
        "limit": 1000,
        "date_from": monday.strftime("%Y-%m-%d"),
        "date_to": friday.strftime("%Y-%m-%d"),
        "sort": "ASC"
    }
    
    # make the API request
    res = req.get(MS_DIVIDENDS_URL, params=params)

    # parse the response data
    # TODO: handle possible errors in response
    try:
        data = res.json() if res.status_code == 200 else None
    except json.JSONDecodeError as e:
        print(f"Error parsing API response to JSON: {e}")
        data = None
        exit(1)

    blob_filename = MS_FILE_FORMAT(MS_DIVIDENDS, monday, friday, MS_DIVIDENDS_RAW_FILE_TYPE)

    # initialize GCS client and specify bucket and blob
    storage_client = storage.Client()
    bucket = storage_client.bucket(MS_DIVIDENDS_RAW_FILE_BUCKET)
    blob = bucket.blob(f'{MS_DIVIDENDS_RAW_FILE_BUCKET_SUBDIR}{blob_filename}')

    # write data to the blob
    blob.upload_from_string(json.dumps(data, indent=4), content_type=f"application/{MS_DIVIDENDS_RAW_FILE_TYPE}")
