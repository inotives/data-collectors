from prefect import flow 
import pandas as pd 

from crypto_data.tasks.kraken import load_ohlc_csv, load_ohlc_api
from crypto_data.tasks.db import insert_data2db
from models.trading_pair_ohlc import TradingOHLC
from utils.epoch import get_current_day_epoch, get_previous_epoch_days

@flow 
def ingest_ohlc_csv(): 
    # Load the data from all the CSV dumps
    data = load_ohlc_csv()

    # Insert the dataframe to trade_ohlc table 
    insert_data2db(TradingOHLC, data, ['uniq_key'])
    
    print(f">>> All CSVs data imported Successfully!!")


@flow 
def ingest_ohlc_api():
    
    # since = get_previous_epoch_days(30)
    since = 1438905600
    trade_pair = 'XBTUSD'
    intervals = 1440

    # load data from API to dataframe 
    data = load_ohlc_api(trade_pair, intervals, since)

    # insert the data to ohlc table 
    insert_data2db(TradingOHLC, data, ['uniq_key'])

    print(">>> Completed data ingestion from REST-API")
