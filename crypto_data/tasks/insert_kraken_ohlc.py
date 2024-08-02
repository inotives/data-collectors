from prefect import task 
import pandas as pd
import pathlib
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

# custom package
from utils.tools import generate_unique_key

# global var 
from configs.settings import POSTGRES_DB_URL

@task
def load_ohlc_csv():
    root_path = pathlib.Path(__file__).parent.parent.parent.resolve()
    data_dir = f"{root_path}/data/csv/kraken_ohlc"
    data_path = pathlib.Path(data_dir)
    source = 'kraken'

    csv_files = list(data_path.glob('*.csv'))

    final_data = []

    for file in csv_files:
        trade_pair, interval = str(file).split("/")[-1].split('.')[0].split("_")
        # load data 
        column_names = ['timestamp','open','high','low','close','volume','count']
        data = pd.read_csv(file, names=column_names)
        
        # Rename the 'trades' column to 'count'
        # data.rename(columns={'trades': 'count'}, inplace=True)

        data['source'] = source
        data['interval'] = interval
        data['trade_pair'] = trade_pair
        data['trade_date'] = pd.to_datetime(data['timestamp'], unit='s')
        data['trade_timestamp'] = data['trade_date'].dt.strftime('%Y-%m-%d %H:%M:%S')
        data['uniq_key'] = data.apply(lambda row: generate_unique_key(row['timestamp'], trade_pair, interval, source), axis=1)

        final_data.append(data)
    
    # Concatenate all DataFrames into a single DataFrame
    final_dataframe = pd.concat(final_data, ignore_index=True)

    return final_dataframe

@task
def insert_data2db(df): 
    engine = create_engine(POSTGRES_DB_URL)
    table_name = 'trading_pair_ohlc'
    try:
        # Insert the DataFrame into the PostgreSQL table
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"Data inserted into table '{table_name}' successfully.")
        return {'status': 'SUCCESS', 'msg': f"data inserted to {table_name}"}
    except SQLAlchemyError as e: 
        print(f"ERROR: {e}")
        return {'status': 'ERROR', 'msg': f"{e}"}
