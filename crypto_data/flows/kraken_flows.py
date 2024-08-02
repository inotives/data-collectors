from prefect import flow 
from crypto_data.tasks.insert_kraken_ohlc import load_ohlc_csv, insert_data2db

@flow 
def ingest_ohlc_csv(): 
    data = load_ohlc_csv()
    tsk = insert_data2db(data)
    
    return tsk

