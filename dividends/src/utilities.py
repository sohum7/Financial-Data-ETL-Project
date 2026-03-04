from datetime import datetime, timedelta

def getCurWkDtRange():
    # today
    run_dt = datetime.today()

    # weekday(): Monday=0, Sunday=6
    monday = run_dt - timedelta(days=run_dt.weekday())  # current week Monday
    friday = monday + timedelta(days=4)                 # current week Friday
    return run_dt, monday.strftime("%Y-%m-%d"), friday.strftime("%Y-%m-%d")

def http_return(http_code: int, msg: str= ""):
    if 200 < http_code < 299:
        return {"status": "success", "message": msg}, http_code
    elif 400 < http_code < 499:
        return {"status": "error", "message": msg}, http_code
    elif 500 < http_code < 599:
        return {"status": "error", "message": msg}, http_code
    
    return {"status": "unknown", "message": msg}, http_code