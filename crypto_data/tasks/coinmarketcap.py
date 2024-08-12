from prefect import task 
import pandas as pd
import pathlib

# custom package
from utils.tools import generate_unique_key
from crypto_data.api.kraken import API

# global var

@task
def load_ohlc_csv():
    root_path = pathlib.Path(__file__).parent.parent.parent.resolve()
    data_dir = f"{root_path}/data/csv/cmc"
    data_path = pathlib.Path(data_dir)
    source = 'cmc'

    csv_files = list(data_path.glob('*.csv'))

    final_data = []

    for file in csv_files:
        asset = str(file).split("_")[0]
        trade_pair = ''
        interval = 1440

        # load data 
        column_names = ['timestamp','open','high','low','close','volume','count']
        data = pd.read_csv(file)
        
        
        # # Data clean up: adding columns and column calculation as per needed
        # # Rename the 'trades' column to 'count'
        # # data.rename(columns={'trades': 'count'}, inplace=True)
        # data['source'] = source
        # data['interval'] = 1440
        # data['trade_pair'] = ''
        # data['asset'] = asset
        # data['vwap'] = None
        # data['trade_date'] = pd.to_datetime(data['timestamp'], unit='s')
        # data['trade_timestamp'] = data['trade_date'].dt.strftime('%Y-%m-%d %H:%M:%S')
        # data['uniq_key'] = data.apply(lambda row: generate_unique_key(row['timeOpen'], trade_pair, interval, source, asset), axis=1)

        final_data.append(data)
    
    # Concatenate all DataFrames into a single DataFrame
    final_dataframe = pd.concat(final_data, ignore_index=True)

    return final_dataframe
@task 
def load_ohlc_api(pair, interval, since):
    api = API()
    columns = ['timestamp', 'open', 'high', 'low', 'close', 'vwap', 'volume', 'count']
    source = 'kraken'

    params = {
        'pair': pair,
        'interval': interval,
        'since': since
    }
    data = api._get_pub_ohlc(params)

    all_rows = []
    for k, vals in data['result'].items():
        if isinstance(vals, list):
            for row in vals: 
                all_rows.append(row)
        else: 
            ''' do nothing '''
    
    df = pd.DataFrame(all_rows, columns=columns)

    # Data cleaning & formatting 
    df['source'] = source
    df['interval'] = interval
    df['trade_pair'] = pair
    df['trade_date'] = pd.to_datetime(df['timestamp'], unit='s')
    df['trade_timestamp'] = df['trade_date'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df['uniq_key'] = df.apply(lambda row: generate_unique_key(row['timestamp'], pair, interval, source), axis=1)

    return df 
