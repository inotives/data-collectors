from prefect import task 

from configs.settings import POSTGRES_DB_URL
from models.base import SqlAlc
from models.trading_pair_ohlc import TradingOHLC

@task 
def create_tables():
    conn = SqlAlc(POSTGRES_DB_URL)
    conn.create_table(TradingOHLC)
    conn.close 