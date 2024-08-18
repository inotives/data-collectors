import sys

# flows
from utils.db import init_db
from crypto_data.flows.kraken_flows import ingest_ohlc_csv, ingest_ohlc_api
from crypto_data.flows.coinmetric_flows import ingest_coinmetric_csv, ingest_coinmetric_api
from crypto_data.flows.etherscan_flows import ingest_etherscan_pyusd_txes
from news_data_collectors.flows.crawler_flows import crawl_news

'''
THIS IS JUST FOR MANUAL START OF THE FLOW, MEANT FOR DEV AND TEST RUN THE FLOW. 
YOU CAN LOOK AT THE DEPLOYMENT FOR HOW TO RUN FLOW WITH SCHEDULER AND AGENTS. 
'''

if __name__ == '__main__':
    flow = sys.argv[1]

    if(flow=='initdb'): 
        init_db()
    elif(flow=='ingest-kraken'):
        ingest_ohlc_csv()
    elif(flow=='ingest-coinmetric'):
        ingest_coinmetric_csv()
    elif(flow=='cm-api'):
        ingest_coinmetric_api('btc,eth,cro,ada,uni,link,matic_eth')
    elif(flow=='kr-api'):
        ingest_ohlc_api()
    elif(flow=='pyusd-tx'):
        ingest_etherscan_pyusd_txes()
    elif(flow=='crawl'):
        crawl_news()
    else: 
        print('NOT a FLOW!!')

