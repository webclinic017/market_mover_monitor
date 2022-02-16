from datetime import datetime
from pytz import timezone

def is_current_date_weekday():
    current_datetime = get_current_datetime()
    is_weekday = True if (current_datetime.weekday() < 5) else False
    return is_weekday

def get_trading_session_start_datetime(current_datetime):
    current_year = current_datetime.year
    current_month = current_datetime.month
    current_day = current_datetime.day

    pre_market_start_datetime = datetime(current_year, current_month, current_day, 4, 0, 0)
    post_market_start_datetime = datetime(current_year, current_month, current_day, 16, 0, 0)
    market_open_datetime = datetime(current_year, current_month, current_day, 9, 30, 0)

    start_datetime = None
    
    if is_premarket_hours(current_datetime):
        start_datetime = pre_market_start_datetime
    elif is_normal_trading_hours(current_datetime):
        start_datetime = market_open_datetime
    elif is_postmarket_hours(current_datetime):
        start_datetime = post_market_start_datetime

    return start_datetime

def is_premarket_hours(current_datetime):
    current_year = current_datetime.year
    current_month = current_datetime.month
    current_day = current_datetime.day

    pre_market_start_datetime = datetime(current_year, current_month, current_day, 4, 1, 0)
    market_open_datetime = datetime(current_year, current_month, current_day, 9, 30, 0)

    if current_datetime >= pre_market_start_datetime and current_datetime < market_open_datetime:
        return True
    else:
        return False

def is_normal_trading_hours(current_datetime):
    current_year = current_datetime.year
    current_month = current_datetime.month
    current_day = current_datetime.day

    market_open_datetime = datetime(current_year, current_month, current_day, 9, 31, 0)
    post_market_start_datetime = datetime(current_year, current_month, current_day, 16, 0, 0)

    if current_datetime >= market_open_datetime and current_datetime < post_market_start_datetime:
        return True
    else:
        return False

def is_postmarket_hours(current_datetime):
    current_year = current_datetime.year
    current_month = current_datetime.month
    current_day = current_datetime.day

    post_market_start_datetime = datetime(current_year, current_month, current_day, 16, 1, 0)
    post_market_end_datetime = datetime(current_year, current_month, current_day, 20, 0, 0)

    if current_datetime >= post_market_start_datetime and current_datetime < post_market_end_datetime:
        return True
    else:
        return False

def get_current_datetime(timezone=timezone('US/Eastern')):
    return datetime.now(timezone).replace(microsecond=0, tzinfo=None)

def convert_datetime_format_str(
            datetime_str: str, 
            parse_format: str = '%Y%m%d %H:%M:%S', 
            convert_format: str = '%Y-%m-%d %H:%M:%S') -> str:
    return datetime.strptime(datetime_str, parse_format).strftime(convert_format)

def parse_datetime_str(datetime_str: str, parse_format: str = '%Y-%m-%d %H:%M:%S'):
    return datetime.strptime(datetime_str, parse_format)