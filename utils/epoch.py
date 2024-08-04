from datetime import datetime, time, timedelta

def get_current_day_epoch():
    current_day = datetime.combine(datetime.today(), time.min)
    return int(current_day.timestamp())

def get_current_time_epoch():
    current = datetime.now 
    return int(current.timestamp())

def get_previous_epoch_days(days):
    current_day = datetime.combine(datetime.today(), time.min)
    prev_datetime = current_day - timedelta(days=days)
    return int(prev_datetime.timestamp())

def get_epoch_datetime(datetime_str):
    datetime_format = '%Y-%m-%d %H:%M:%S'
    datetime_obj = datetime.strptime(datetime_str, datetime_format)

    return int(datetime_obj.timestamp())

def get_timestamp_from_epoch (epoch):
    
    # Convert the epoch timestamp to a datetime object
    datetime_obj = datetime.fromtimestamp(epoch)
    datetime_format = '%Y-%m-%d %H:%M:%S'

    return datetime_obj.strftime(datetime_format)
    