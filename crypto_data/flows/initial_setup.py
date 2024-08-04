from prefect import flow 
from crypto_data.tasks.db import create_table

# Models 
from models.trading_pair_ohlc import TradingOHLC
from models.coinmetric_data import CoinmetricDaily

@flow 
def create_all_table_needed(): 
    create_table(TradingOHLC)
    create_table(CoinmetricDaily)
    print(">>> All tables successfully created!!")


