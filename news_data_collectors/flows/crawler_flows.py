import time
from prefect import flow 

from news_data_collectors.tasks.crawlers import crawl_cointelegraph, crawl_cryptonews, crawl_investingcom_news
from models.news_articles import NewsArticles
from utils.db import insert_data2db

@flow
def crawl_news():

    # Crawlers 

    print("Start Crawlers for CoinTelegraph ...")
    df_cointelegraph = crawl_cointelegraph()
    print('>> Storing Cointelegraph to DB')
    status_1 = insert_data2db(NewsArticles, df_cointelegraph, ['uniq_key'])
    print(f">> All News Articles inserted. \n>> {status_1}")
    time.sleep(2)

    print("Start Crawlers for Crypto.news ...")
    df_cryptonews = crawl_cryptonews()
    print('>> Storing CryptoNews to DB')
    status_2 = insert_data2db(NewsArticles, df_cryptonews, ['uniq_key'])
    print(f">> All News Articles inserted. \n>> {status_2}")
    time.sleep(2)
    
    print("Start Crawlers for Investing.com ...")
    df_investingcom = crawl_investingcom_news()
    print('>> Storing Investing.com News to DB')
    status_3 = insert_data2db(NewsArticles, df_investingcom, ['uniq_key'])
    print(f">> All News Articles inserted. \n>> {status_3}")
    time.sleep(2)





