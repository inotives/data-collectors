from prefect import task
import pandas as pd 
import pathlib
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError 

# custom package/lib 
from utils.tools import generate_unique_key
from crypto_data.api.coinmetric import API


COL_NEEDED = [
    'asset_symbol', 'uniq_key', 'time','AdrActCnt','CapAct1yrUSD','CapMVRVCur','CapMVRVFF','CapMrktCurUSD',
    'CapMrktEstUSD','CapMrktFFUSD','CapRealUSD','FeeMeanNtv','FeeMeanUSD','FeeMedNtv',
    'FeeMedUSD','FeeTotNtv','FeeTotUSD','PriceBTC','PriceUSD','SplyFF','SplyCur','TxCnt',
    'TxTfrCnt','TxTfrValAdjNtv','TxTfrValAdjUSD','TxTfrValMeanNtv','TxTfrValMeanUSD',
    'TxTfrValMedNtv','TxTfrValMedUSD','SER','AdrBalUSD100Cnt','AdrBalUSD1KCnt',
    'AdrBalUSD10KCnt','AdrBalUSD100KCnt','AdrBalUSD1MCnt','SplyAdrBalUSD100',
    'SplyAdrBalUSD1K','SplyAdrBalUSD10K','SplyAdrBalUSD100K','SplyAdrBalUSD1M','SplyAct1d',
    'SplyAct30d','SplyAct1yr','SplyAct90d','VtyDayRet30d'
]

@task 
def load_coinmetric_csv(): 
    root_path = pathlib.Path(__file__).parent.parent.parent.resolve()
    data_dir = f"{root_path}/data/csv/coinmetric"
    data_path = pathlib.Path(data_dir)

    csv_files = list(data_path.glob('*.csv'))

    final_data = []

    for file in csv_files:
        asset_symbol = str(file).split("/")[-1].split('.')[0]

        data = pd.read_csv(file)
        
        # Find any columns that not exist in the require cols
        missing_col = [col for col in COL_NEEDED if col not in data.columns]
        
        # insert the missing col into the dataframe
        for col in missing_col: 
            data[col] = None 
        data['asset_symbol'] = asset_symbol
        data['uniq_key'] = data.apply(lambda row: generate_unique_key(row['time'], asset_symbol), axis=1)

        # Subset the table to only those columns that are required
        adjusted_df = data[COL_NEEDED]

        final_data.append(adjusted_df)

    # Concatenate all dataframe into single dataframe 
    final_dataframe = pd.concat(final_data, ignore_index=True)

    return final_dataframe

@task
def get_coinmetric_daily_api(asset_list, start_date, end_date):
    
    api = API()
    data = api._get_coinmetric_daily(asset_list, start_date,end_date)
    data_df = pd.DataFrame(data['data'])

    # formatting timestamp string to just date 
    data_df['timestamp'] = pd.to_datetime(data_df['time'])
    data_df['time'] = data_df['timestamp'].dt.date
    data_df.drop(columns=['timestamp'], inplace=True)    

    # adding uniq key
    data_df['uniq_key'] = data_df.apply(lambda row: generate_unique_key(row['time'], row['asset']), axis=1)
    
    # renaming asset > asset_symbol
    data_df.rename(columns={'asset': 'asset_symbol'}, inplace=True)
    
    # check if there are any missing col compare to require metrics 
    missing_col = [col for col in COL_NEEDED if col not in data_df.columns]

    if len(missing_col) == 0: 
        return data_df
    else: 
        print(f"MISSING_COL::{missing_col}")
        return None


