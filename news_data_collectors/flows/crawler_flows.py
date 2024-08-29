import time
from prefect import flow 

import news_data_collectors.tasks.crawlers as cr_task
from models.news_articles import NewsArticles
from utils.db import insert_data2db

@flow
def crawl_news():

    # Crawlers 

    print("Start Crawlers for CoinTelegraph ...")
    df_cointelegraph = cr_task.crawl_cointelegraph()
    print('>> Storing Cointelegraph to DB')
    status_1 = insert_data2db(NewsArticles, df_cointelegraph, ['uniq_key'])
    print(f">> All News Articles inserted. \n>> {status_1}")
    time.sleep(2)

    print("Start Crawlers for Crypto.news ...")
    df_cryptonews = cr_task.crawl_cryptonews()
    print('>> Storing CryptoNews to DB')
    status_2 = insert_data2db(NewsArticles, df_cryptonews, ['uniq_key'])
    print(f">> All News Articles inserted. \n>> {status_2}")
    time.sleep(2)
    
    print("Start Crawlers for Investing.com ...")
    df_investingcom = cr_task.crawl_investingcom_news()
    print('>> Storing Investing.com News to DB')
    status_3 = insert_data2db(NewsArticles, df_investingcom, ['uniq_key'])
    print(f">> All News Articles inserted. \n>> {status_3}")
    time.sleep(2)

    print("Start Crawlers for u.today ...")
    df_utoday = cr_task.crawl_utoday_news()
    print('>> Storing U.Today News to DB')
    status_4 = insert_data2db(NewsArticles, df_utoday, ['uniq_key'])
    print(f">> All News Articles inserted. \n>> {status_4}")
    time.sleep(2)

    # print("Start Crawlers for Unchained Crypto ...")
    # df_utoday = cr_task.crawl_unchainedcrypto_news()
    # print('>> Storing Unchained Crypto News to DB')
    # status_5 = insert_data2db(NewsArticles, df_utoday, ['uniq_key'])
    # print(f">> All News Articles inserted. \n>> {status_5}")
    # time.sleep(2)



