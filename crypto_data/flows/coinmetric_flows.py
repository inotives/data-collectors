from prefect import flow
from datetime import datetime, timedelta

# task
from crypto_data.tasks.coinmetric import load_coinmetric_csv
from utils.db import insert_data2db 
from crypto_data.tasks.coinmetric import get_coinmetric_daily_api

# models
from models.coinmetric_data import CoinmetricDaily

@flow 
def ingest_coinmetric_csv(): 
    data = load_coinmetric_csv()
    status = insert_data2db(CoinmetricDaily,data,['uniq_key'])
    print(f">>> Ingested all CSVs from Coinmetric Data dump!!. \n {status}")


@flow
def ingest_coinmetric_api(asset_symbol: str):

    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() -  timedelta(days=3)).strftime('%Y-%m-%d')

    # get data from API
    data = get_coinmetric_daily_api(asset_symbol, start_date, end_date)

    # Store the data into postgress
    status = insert_data2db(CoinmetricDaily, data, ['uniq_key'])
    
    print(f"insert from API Success!! \n{status}")
