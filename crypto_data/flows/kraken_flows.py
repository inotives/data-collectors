from prefect import flow 
import pandas as pd 

from crypto_data.tasks.kraken import load_ohlc_csv, load_ohlc_api
from utils.db import insert_data2db
from models.trading_pair_ohlc import TradingOHLC

@flow 
def ingest_ohlc_csv(): 
    # Load the data from all the CSV dumps
    data = load_ohlc_csv()

    # Insert the dataframe to trade_ohlc table 
    status = insert_data2db(TradingOHLC, data, ['uniq_key'])
    
    print(f">>> All CSVs data imported Successfully!! \n{status}")


@flow 
def ingest_ohlc_api():
    
    # since = get_previous_epoch_days(30)
    since = 1438905600
    trade_pair = 'XBTUSD'
    intervals = 1440

    # load data from API to dataframe 
    data = load_ohlc_api(trade_pair, intervals, since)

    # insert the data to ohlc table 
    status = insert_data2db(TradingOHLC, data, ['uniq_key'])

    print(f">>> Completed data ingestion from REST-API. \n{status}")
