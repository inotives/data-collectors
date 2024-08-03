from prefect import task 

from configs.settings import POSTGRES_DB_URL
from models.base import SqlAlc
from models.trading_pair_ohlc import TradingOHLC
from models.coinmetric_data import CoinmetricDaily
from sqlalchemy.exc import SQLAlchemyError

@task 
def create_table_ohlc():
    try: 
        conn = SqlAlc(POSTGRES_DB_URL)
        conn.create_table(TradingOHLC)
        conn.close 
        print('Table:: trading_ohlc created.')
        return {'status': 'Success', 'msg': 'ohlc table created'}
        
    except SQLAlchemyError as e: 
        print(f"ERROR: {e}")

def create_table_coinmetrics_data():
    try: 
        conn = SqlAlc(POSTGRES_DB_URL)
        conn.create_table(CoinmetricDaily)
        conn.close
        print('Table:: coinmetric_data created.')
        return {'status': 'Success', 'msg': 'coinmetric_daily table created'}
    
    except SQLAlchemyError as e: 
        print(f"ERROR: {e}")

    