from datetime import datetime, timedelta

def getCurWkDtRange():
    # today
    today = datetime.today()

    # weekday(): Monday=0, Sunday=6
    monday = today - timedelta(days=today.weekday())  # current week Monday
    friday = monday + timedelta(days=4)              # current week Friday
    return monday, friday