from datetime import datetime, timedelta
import requests as req
import json
from google.cloud import storage

MS_DIVIDENDS_API_KEY = "25d2f25478972551751d275bd4048f10"
MS_DIVIDENDS_URL = "https://api.marketstack.com/v2/dividends"

MS_DIVIDENDS_DATA_BUCKET = "market-stack-data-dev"
MS_DIVIDENDS_DATA_BUCKET_SUBDIR = "bronze/dividends/"


def getCurWkDtRange():
    # today
    today = datetime.today()

    # weekday(): Monday=0, Sunday=6
    monday = today - timedelta(days=today.weekday())  # current week Monday
    friday = monday + timedelta(days=4)              # current week Friday
    return monday, friday



def main():
    # company symbols from which to extract data from
    symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "FB", "TSLA", "NVDA"]
    symbols_params_str = ",".join(symbols)
    
    monday, friday = getCurWkDtRange()
    
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

    blob_filename = f"dividend_data_{monday.strftime('%Y%m%d')}_{friday.strftime('%Y%m%d')}.json"

    # initialize GCS client and specify bucket and blob
    storage_client = storage.Client()
    bucket = storage_client.bucket(MS_DIVIDENDS_DATA_BUCKET)
    blob = bucket.blob(f'{MS_DIVIDENDS_DATA_BUCKET_SUBDIR}{blob_filename}')

    # write data to the blob
    blob.upload_from_string(json.dumps(data, indent=4), content_type="application/json")



pass