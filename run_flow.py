import sys
from crypto_data.flows.create_all_require_tables import create_all_table_needed
from crypto_data.flows.kraken_flows import ingest_ohlc_csv


if __name__ == '__main__':
    flow = sys.argv[1]
    if(flow=='create-tables'): 
        create_all_table_needed()
    elif(flow=='ingest-kraken'):
        ingest_ohlc_csv()
    else: 
        print('NOT a FLOW!!')


# from configs.settings import POSTGRES_DB_URL

# print(POSTGRES_DB_URL)