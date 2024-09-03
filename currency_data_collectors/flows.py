from prefect import flow 

from currency_data_collectors.tasks import crawl_sgrates_sites
from models.fiat_sgd_rates import FiatSGDRates
from utils.db import insert_data2db

@flow
def scrap_sgrate_currency_data(start, end):

    # DBS 
    df_dbs = crawl_sgrates_sites(start, end)
    # insert to db
    insert_data2db(FiatSGDRates, df_dbs, ['uniq_key'])