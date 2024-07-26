from prefect import flow 
from crypto_data.tasks.create_tables import create_tables

@flow 
def create_all_table_needed(): 
    new_table = create_tables()
    return new_table

