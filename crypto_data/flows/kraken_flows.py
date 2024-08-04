from prefect import flow 
from crypto_data.tasks.kraken import load_ohlc_csv
from crypto_data.tasks.db import insert_data2db
from models.trading_pair_ohlc import TradingOHLC

@flow 
def ingest_ohlc_csv(): 
    # Load the data from all the CSV dumps
    data = load_ohlc_csv()

    # Insert the dataframe to trade_ohlc table 
    insert_data2db(TradingOHLC, data, ['uniq_key'])
    
    print(f">>> All CSVs data imported Successfully!!")


@flow 
def ingest_ohlc_api():
    
    # load data from API to dataframe 

    # insert the data to ohlc table 

    print(">>> Completed data ingestion from REST-API")
