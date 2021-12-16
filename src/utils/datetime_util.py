from datetime import datetime
from pytz import timezone

def convert_hms_to_second(time_str):
    h, m, s = time_str.split(':')
    return int(h) * 3600 + int(m) * 60 + int(s)

def is_current_date_weekday(timezone):
    current_date_time = datetime.now(timezone).replace(microsecond=0, tzinfo=None)
    is_weekday = True if (current_date_time.weekday() < 5) else False
    return is_weekday

def get_trading_interval(timezone=timezone('US/Eastern')):
    current_date_time = datetime.now(timezone).replace(microsecond=0, tzinfo=None)

    current_year = current_date_time.year
    current_month = current_date_time.month
    current_day = current_date_time.day

    pre_market_start_datetime = datetime(current_year, current_month, current_day, 4, 0, 0)
    post_market_start_datetime = datetime(current_year, current_month, current_day, 16, 0, 0)
    market_open_datetime = datetime(current_year, current_month, current_day, 9, 30, 0)

    if is_premarket_hours(current_date_time):
        start_datetime = pre_market_start_datetime
    elif is_normal_trading_hours(current_date_time):
        start_datetime = market_open_datetime
    elif is_postmarket_hours(current_date_time):
        start_datetime = post_market_start_datetime
    else:
        raise Exception(f'Current datetime is not trading hours, current datetime: {current_date_time}')
    
    interval = current_date_time - start_datetime
    return convert_hms_to_second(str(interval))

def is_premarket_hours(current_date_time, timezone=timezone('US/Eastern')):
    current_date_time = datetime.now(timezone).replace(microsecond=0, tzinfo=None)

    current_year = current_date_time.year
    current_month = current_date_time.month
    current_day = current_date_time.day

    pre_market_start_datetime = datetime(current_year, current_month, current_day, 4, 0, 0)
    market_open_datetime = datetime(current_year, current_month, current_day, 9, 30, 0)

    if current_date_time > pre_market_start_datetime and current_date_time < market_open_datetime:
        return True
    else:
        return False

def is_normal_trading_hours(current_date_time):
    current_year = current_date_time.year
    current_month = current_date_time.month
    current_day = current_date_time.day

    market_open_datetime = datetime(current_year, current_month, current_day, 9, 30, 0)
    post_market_start_datetime = datetime(current_year, current_month, current_day, 16, 0, 0)

    if current_date_time > market_open_datetime and current_date_time < post_market_start_datetime:
        return True
    else:
        return False

def is_postmarket_hours(current_date_time):
    current_year = current_date_time.year
    current_month = current_date_time.month
    current_day = current_date_time.day

    post_market_start_datetime = datetime(current_year, current_month, current_day, 16, 0, 0)
    post_market_end_datetime = datetime(current_year, current_month, current_day, 20, 0, 0)

    if current_date_time > post_market_start_datetime and current_date_time < post_market_end_datetime:
        return True
    else:
        return False

def convert_datetime_format_str(
            datetime_str: str, 
            parse_format: str = '%Y%m%d %H:%M:%S', 
            convert_format: str = '%Y-%m-%d %H:%M:%S') -> str:
    return datetime.strptime(datetime_str, parse_format).strftime(convert_format)