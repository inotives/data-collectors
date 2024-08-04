from prefect import task
import pandas as pd 
import pathlib
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError 

# custom package/lib 
from utils.tools import generate_unique_key

@task 
def load_coinmetric_csv(): 
    root_path = pathlib.Path(__file__).parent.parent.parent.resolve()
    data_dir = f"{root_path}/data/csv/coinmetric"
    data_path = pathlib.Path(data_dir)

    csv_files = list(data_path.glob('*.csv'))
    col_needed = [
        'asset_symbol', 'uniq_key', 'time','AdrActCnt','CapAct1yrUSD','CapMVRVCur','CapMVRVFF','CapMrktCurUSD',
        'CapMrktEstUSD','CapMrktFFUSD','CapRealUSD','FeeMeanNtv','FeeMeanUSD','FeeMedNtv',
        'FeeMedUSD','FeeTotNtv','FeeTotUSD','PriceBTC','PriceUSD','SplyFF','SplyCur','TxCnt',
        'TxTfrCnt','TxTfrValAdjNtv','TxTfrValAdjUSD','TxTfrValMeanNtv','TxTfrValMeanUSD',
        'TxTfrValMedNtv','TxTfrValMedUSD','SER','AdrBalUSD100Cnt','AdrBalUSD1KCnt',
        'AdrBalUSD10KCnt','AdrBalUSD100KCnt','AdrBalUSD1MCnt','SplyAdrBalUSD100',
        'SplyAdrBalUSD1K','SplyAdrBalUSD10K','SplyAdrBalUSD100K','SplyAdrBalUSD1M','SplyAct1d',
        'SplyAct30d','SplyAct1yr','SplyAct90d','VtyDayRet30d'
    ]

    final_data = []

    for file in csv_files:
        asset_symbol = str(file).split("/")[-1].split('.')[0]

        data = pd.read_csv(file)
        
        # Find any columns that not exist in the require cols
        missing_col = [col for col in col_needed if col not in data.columns]
        
        # insert the missing col into the dataframe
        for col in missing_col: 
            data[col] = None 
        data['asset_symbol'] = asset_symbol
        data['uniq_key'] = data.apply(lambda row: generate_unique_key(row['time'], asset_symbol), axis=1)

        # Subset the table to only those columns that are required
        adjusted_df = data[col_needed]

        final_data.append(adjusted_df)

    # Concatenate all dataframe into single dataframe 
    final_dataframe = pd.concat(final_data, ignore_index=True)

    return final_dataframe


