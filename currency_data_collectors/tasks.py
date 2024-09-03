from prefect import task
import pandas as pd
import time 

import utils.web_crawlers as crw
import utils.tools as tool 


@task
def crawl_sgrates_sites(start_date, end_date, bank='dbs', loop_gap=2):
    
    datestrs = tool.generate_date_list(start_date, end_date)
    data_list = []
    for d in datestrs: 
        df = crw.crawl_sgrates(d, bank)
        data_list.append(df)
        print(f"---> Crawling:{bank}, date:{d}")
        time.sleep(loop_gap)
    
    final_df = pd.concat(data_list, ignore_index=True)
    
    # Replace NaN with None
    final_df.drop_duplicates(subset=['uniq_key'], inplace=True)

    return final_df