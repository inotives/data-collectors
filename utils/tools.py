from datetime import datetime, time, timedelta
import hashlib 
import pathlib
import pandas as pd
import json

ROOT_DIR = pathlib.Path(__file__).parent.parent.resolve()
DATA_DIR = f"{ROOT_DIR}/data"


def generate_unique_key(*args):
    """
    Generates a unique key using the provided variables.
    Args:
        *args: Any number of variables to be used in the key generation.
    Returns:
        A unique key as a hexadecimal string.
    """
    # Concatenate string representations of all arguments
    concatenated_string = ''.join(map(str, args))
    
    # Generate a unique key using SHA-256 hash
    unique_key = hashlib.sha256(concatenated_string.encode()).hexdigest()
    
    return unique_key


def get_current_day_epoch():
    """ Get current day date in unix epoch int """
    current_day = datetime.combine(datetime.today(), time.min)
    return int(current_day.timestamp())


def get_current_time_epoch():
    """ Get current day timestamp in unix epoch int """
    current = datetime.now 
    return int(current.timestamp())


def get_previous_epoch_days(days):
    """ Get previous day date in unix timestamp """
    current_day = datetime.combine(datetime.today(), time.min)
    prev_datetime = current_day - timedelta(days=days)
    return int(prev_datetime.timestamp())


def get_epoch_datetime(datetime_str='2020-01-01 00:00:00'):
    """ Convert datetime str to Unix timestamp. Format str: YYYY-MM-DD hh:mm:ss """
    datetime_format = '%Y-%m-%d %H:%M:%S'
    datetime_obj = datetime.strptime(datetime_str, datetime_format)

    return int(datetime_obj.timestamp())


def get_timestamp_from_epoch(epoch):
    """Convert unix timestamp to Datestring formate: YYYY-MM-DD hh:mm:ss """
    # Convert the epoch timestamp to a datetime object
    datetime_obj = datetime.fromtimestamp(epoch)
    datetime_format = '%Y-%m-%d %H:%M:%S'

    return datetime_obj.strftime(datetime_format)


def load_csv_from_data(csv_file):
    """Importing Data from CSV """    
    file_path = f"{DATA_DIR}/csv/{csv_file}.csv"

    return pd.read_csv(file_path)


def export_csv_to_data(data, filename):
    """Exporting Data to CSV"""

    exported_dir = f"{DATA_DIR}/csv/_OUTPUT_{filename}.csv"
    
    data.to_csv(exported_dir, index=False)
    
    print(f">> Data exported to :: {exported_dir}")

    return 


def process_json_str(json_str):
    """convert json string to dataframe """
    data = json.loads(json_str) # parse json str into python
    df = pd.json_normalize(data)

    return df 


def process_large_json(file_path, chunksize=1000):
    """Processing large json file by chunking"""
    # Initialize an empty list to hold chunks of DataFrames
    chunks = []
    
    # Open the large JSON file and load it in chunks
    with open(file_path, 'r') as file:
        # Load the JSON data as a list of dictionaries
        data = json.load(file)
        
        # Process the data in chunks
        for i in range(0, len(data), chunksize):
            chunk = data[i:i+chunksize]
            
            # Convert each chunk to a DataFrame
            df_chunk = pd.DataFrame(chunk)
            
            # Append the chunk to the list
            chunks.append(df_chunk)
    
    # Concatenate all the chunks into a single DataFrame
    full_df = pd.concat(chunks, ignore_index=True)
    
    return full_df

