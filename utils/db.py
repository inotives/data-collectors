from prefect import task, flow

# custom
from configs.settings import POSTGRES_DB_URL
from models.base import SqlAlc

# Models 
from models.trading_pair_ohlc import TradingOHLC
from models.coinmetric_data import CoinmetricDaily
from models.ethereum_token_txes import EthTokenTxes
from models.news_articles import NewsArticles

''' -- TASKS ------------------------------------------------------------- '''
@task 
def create_table(table_class):    
    conn = SqlAlc(POSTGRES_DB_URL)
    
    status = conn.create_table(table_class)
    print(status)

    conn.close()
    
@task 
def insert_data2db(table_class, data, conflicts): 
    conn = SqlAlc(POSTGRES_DB_URL)
    
    status = conn.upsert_data(table_class, data, conflicts)
    print(status)

    conn.close()

@task
def export_data2csv(table_class, output_file='output', filters=None, date_field=None, date_range=None):
    conn = SqlAlc(POSTGRES_DB_URL)

    status = conn.export_to_csv(
        table_class, 
        output_file=output_file, 
        filters=filters,
        date_range=date_range,
        date_field=date_field
    )

    return status

     
''' -- FLOWS ------------------------------------------------------------- '''
@flow
def init_db():
    create_table(TradingOHLC)
    create_table(CoinmetricDaily)
    create_table(EthTokenTxes)
    create_table(NewsArticles)
    print(">>> All tables successfully created!!")