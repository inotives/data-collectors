from prefect import flow 
import pandas as pd
import time 

import crypto_data.tasks.etherscan as etherscan
from utils.db import insert_data2db
from models.ethereum_token_txes import EthTokenTxes


# pd.set_option('display.max_colwidth', None)
# pd.set_option('display.max_columns', None)

@flow
def ingest_etherscan_txes_historical(contr_addr, tk_symbol, loop_cnt=1):
    
    # get data from rest api 
    for i in range(loop_cnt):
        data = etherscan.get_etherscan_token_txes(contr_addr, tk_symbol)

        final_data = data.drop_duplicates()

        insert_data2db(EthTokenTxes, final_data, ['uniq_key'])

        time.sleep(10)

    return '>> Historical Ran :: COMPLETED.'

@flow
def ingest_etherscan_txes_latest():

    tokens = [
        ['PAXG', '0x45804880De22913dAFE09f4980848ECE6EcbAf78'],
        ['PYUSD', '0x6c3ea9036406852006290770BEdFcAbA0e23A0e8']
    ]

    data_list = []
    for token in tokens:
        data = etherscan.get_etherscan_token_txes(token[1], token[0])
        data_list.append(data)
    
    final_df = pd.concat(data_list)
    final_df.drop_duplicates(inplace=True)

    status = insert_data2db(EthTokenTxes, final_df, ['uniq_key'])

    return status