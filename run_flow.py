import sys

# flows
from utils.db import init_db
from crypto_data.flows.kraken_flows import ingest_ohlc_csv, ingest_ohlc_api
from crypto_data.flows.coinmetric_flows import ingest_coinmetric_csv, ingest_coinmetric_api
import crypto_data.flows.etherscan_flows as eth
from crypto_data.flows.coinmarketcap_flows import ingest_ohlcv_cmc
from news_data_collectors.flows.crawler_flows import crawl_news

'''
THIS IS JUST FOR MANUAL START OF THE FLOW, MEANT FOR DEV AND TEST RUN THE FLOW. 
YOU CAN LOOK AT THE DEPLOYMENT FOR HOW TO RUN FLOW WITH SCHEDULER AND AGENTS. 
'''

if __name__ == '__main__':
    arg_len = len(sys.argv)
    
    flow = sys.argv[1]
    
    if arg_len > 2:
        extra = sys.argv[2:]

    if(flow=='initdb'): 
        init_db()
    elif(flow=='ingest-kraken'):
        ingest_ohlc_csv()
    elif(flow=='ingest-coinmetric'):
        ingest_coinmetric_csv()
    elif(flow=='ingest-cmc'):
        ingest_ohlcv_cmc()
    elif(flow=='cm-api'):
        ingest_coinmetric_api('btc,eth,cro,ada,uni,link,matic_eth')
    elif(flow=='kr-api'):
        ingest_ohlc_api()
    elif(flow=='tokentx'):
        if(extra[0] == 'hist'):
            contr='0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
            token='USDC'
            cnt=int(extra[1])
            eth.ingest_etherscan_txes_historical(contr_addr=contr, tk_symbol=token, loop_cnt=cnt)
        else:    
            eth.ingest_etherscan_txes_latest()
    elif(flow=='crawl'):
        crawl_news()
    else: 
        print('NOT a FLOW!!')

