import sys

# flows
from crypto_data.flows.initial_setup import create_all_table_needed
from crypto_data.flows.kraken_flows import ingest_ohlc_csv, ingest_ohlc_api
from crypto_data.flows.coinmetric_flows import ingest_coinmetric_csv, ingest_coinmetric_api

'''
THIS IS JUST FOR MANUAL START OF THE FLOW, MEANT FOR DEV AND TEST RUN THE FLOW. 
YOU CAN LOOK AT THE DEPLOYMENT FOR HOW TO RUN FLOW WITH SCHEDULER AND AGENTS. 
'''

if __name__ == '__main__':
    flow = sys.argv[1]

    if(flow=='create-tables'): 
        create_all_table_needed()
    elif(flow=='ingest-kraken'):
        ingest_ohlc_csv()
    elif(flow=='ingest-coinmetric'):
        ingest_coinmetric_csv()
    elif(flow=='cm-api'):
        ingest_coinmetric_api('btc,eth,cro,ada,uni,link,matic_eth')
    elif(flow=='kr-api'):
        ingest_ohlc_api()
    else: 
        print('NOT a FLOW!!')

