from prefect import task 
import pandas as pd 

from crypto_data.api.etherscan import EtherscanApi 
from utils.tools import generate_unique_key
from utils.db import etherscan_tokentx_get_latest_blocknum

@task 
def get_etherscan_token_txes(contr_addr, asset):

    latest_blocknum = etherscan_tokentx_get_latest_blocknum(asset=asset)
    block_num = latest_blocknum if latest_blocknum else 0
    print(f">>>> Starting BlockNum: {block_num}")

    api = EtherscanApi()
    data = api._get_erc20_txes(contract_addr=contr_addr, startb=block_num)
    
    data_df = pd.DataFrame(data['result'])
    print(f">>>> Total txes pulled: {len(data_df)}")

    # column_mapping: change the column names to match the db
    cols = {
        'blockNumber': 'block_number',
        'timeStamp': 'timestamp',
        'hash': 'transaction_hash',
        'blockHash': 'block_hash',
        'from': 'from_addr',
        'contractAddress': 'contract_addr',
        'to': 'to_addr',
        'value': 'transaction_value',
        'tokenName': 'token_name',
        'tokenSymbol': 'token_symbol',
        'tokenDecimal': 'token_decimal',
        'transactionIndex': 'transaction_index',
        'gasPrice': 'gas_price',
        'gasUsed': 'gas_used',
        'cumulativeGasUsed': 'cumulative_gas_used',
        'input': 'transaction_input'
    }
    data_df.rename(columns=cols, inplace=True)
    
    # Adding other important columns 
    data_df['transaction_date'] = pd.to_datetime(data_df['timestamp'], unit='s')
    data_df['transaction_timestamp'] = data_df['transaction_date'].dt.strftime('%Y-%m-%d %H:%M:%S')
    data_df['uniq_key'] = data_df.apply(lambda row: generate_unique_key(row['timestamp'], row['token_symbol'], row['transaction_hash'], row['from_addr'], row['to_addr'], row['transaction_value']), axis=1)

    # print(data_df.info())

    return data_df