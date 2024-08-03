from prefect import flow 
from crypto_data.tasks.create_tables import create_table_ohlc, create_table_coinmetrics_data

@flow 
def create_all_table_needed(): 
    create_table_ohlc()
    create_table_coinmetrics_data()
    print("success created!!")


