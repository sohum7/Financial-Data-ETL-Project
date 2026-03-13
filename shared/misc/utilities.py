# Utility functions for the dividends ETL pipeline, including date range calculations and standardized HTTP response formatting.

# Builtin imports
from datetime import datetime, date, timedelta

def get_past_week_range(reference_date: date | None=None):
    if reference_date is None:
        reference_date = date.today()
    if not isinstance(reference_date, date):
        raise ValueError("reference_date is not of type date or None")
    # weekday(): Monday=0, Sunday=6
    # Convert to Sunday=0, Saturday=6
    weekday_sun0 = (reference_date.weekday() + 1) % 7
    
    # If today is Saturday (weekday_sun0 == 6), we include it in the past week
    # Last Saturday is reference_date if Saturday else previous Saturday
    last_saturday = reference_date if weekday_sun0 == 6 else reference_date - timedelta(days=weekday_sun0 + 1)
    
    # Sunday is 6 days before Saturday
    last_sunday = last_saturday - timedelta(days=6)
    
    return reference_date.strftime("%Y-%m-%d"), last_sunday.strftime("%Y-%m-%d"), last_saturday.strftime("%Y-%m-%d")

def http_return(http_code: int, msg: str= ""):
    if 200 < http_code < 299:
        return {"status": "success", "message": msg}, http_code
    elif 400 < http_code < 499:
        return {"status": "error", "message": msg}, http_code
    elif 500 < http_code < 599:
        return {"status": "error", "message": msg}, http_code
    
    return {"status": "unknown", "message": msg}, http_code