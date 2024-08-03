from prefect import flow 
from crypto_data.tasks.coinmetric import load_coinmetric_csv
from crypto_data.tasks.db import insert_data2db 

from models.coinmetric_data import CoinmetricDaily

@flow 
def ingest_coinmetric_csv(): 
    data = load_coinmetric_csv()
    
    insert_data2db(CoinmetricDaily,data)
 