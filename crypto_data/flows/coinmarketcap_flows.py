from prefect import flow
from crypto_data.tasks.coinmarketcap import load_ohlc_csv
from utils.db import insert_data2db
from models.cmc_ohlcv import CMCOHLCV

@flow
def ingest_ohlcv_cmc():
    data = load_ohlc_csv()

    status = insert_data2db(CMCOHLCV, data, ['uniq_key'])
    
    print(f">>> All Data imported Successfully!! \n{status}")