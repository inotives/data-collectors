from prefect import flow 
from crypto_data.tasks.db import create_table

# Models 
from models.trading_pair_ohlc import TradingOHLC
from models.coinmetric_data import CoinmetricDaily
from models.ethereum_token_txes import EthTokenTxes

@flow 
def create_all_table_needed(): 
    create_table(TradingOHLC)
    create_table(CoinmetricDaily)
    create_table(EthTokenTxes)
    print(">>> All tables successfully created!!")


