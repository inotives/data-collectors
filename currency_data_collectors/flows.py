from prefect import flow 

from currency_data_collectors.tasks import crawl_sgrates_sites
from models.fiat_sgd_rates import FiatSGDRates
from utils.db import insert_data2db

@flow
def scrap_sgrate_currency_data():

    # DBS 
    df_dbs = crawl_sgrates_sites('2024-01-01', '2024-01-05', 'dbs')
    # insert to db
    insert_data2db(FiatSGDRates, df_dbs, ['uniq_key'])