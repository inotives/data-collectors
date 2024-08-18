import time
from prefect import flow 

from news_data_collectors.tasks.cointelegraph import crawl_cointelegraph, crawl_cryptonews
from models.news_articles import NewsArticles
from utils.db import insert_data2db

@flow
def crawl_news():

    # Crawlers 
    df_cointelegraph = crawl_cointelegraph()
    print('>> Storing Cointelegraph to DB')
    status_1 = insert_data2db(NewsArticles, df_cointelegraph, ['uniq_key'])
    print(f">> All News Articles inserted. \n>> {status_1}")

    print(f">> Stop for 5 Sec ...")
    time.sleep(5)

    df_cryptonews = crawl_cryptonews()
    print('>> Storing CryptoNews to DB')
    status_2 = insert_data2db(NewsArticles, df_cryptonews, ['uniq_key'])
    print(f">> All News Articles inserted. \n>> {status_2}")



