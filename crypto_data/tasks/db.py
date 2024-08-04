from prefect import task 
import pandas as pd

# custom
from configs.settings import POSTGRES_DB_URL
from models.base import SqlAlc

@task 
def create_table(table_class):    
    conn = SqlAlc(POSTGRES_DB_URL)
    conn.create_table(table_class)
    conn.close()
    
@task 
def insert_data2db(table_class, data, conflicts): 
    conn = SqlAlc(POSTGRES_DB_URL)
    conn.upsert_data(table_class, data, conflicts)
    conn.close()
     

