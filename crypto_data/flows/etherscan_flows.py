from prefect import flow 
import pandas as pd

from crypto_data.tasks.etherscan import get_pyusd_txes
from crypto_data.tasks.db import insert_data2db

from models.ethereum_token_txes import EthTokenTxes

@flow
def ingest_etherscan_pyusd_txes():
    
    pd.set_option('display.max_colwidth', None)
    pd.set_option('display.max_columns', None)

    # get data from rest api 
    data = get_pyusd_txes()

    final_data = data.drop_duplicates()

    insert_data2db(EthTokenTxes, final_data, ['uniq_key'])

    print('Data End')