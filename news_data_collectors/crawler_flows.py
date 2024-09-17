import time
from prefect import flow 

import news_data_collectors.crawler_tasks as cr_task
from models.news_articles import NewsArticles
import utils.db as db

@flow
def crawl_news():
    col2update = ['uniq_key', 'source', 'title', 'content', 'link', 'article_date', 'author', 'tag', 'captured_at']
    # Crawlers 

    print("Start Crawlers for CoinTelegraph ...")
    df_cointelegraph = cr_task.crawl_cointelegraph()
    print('>> Storing Cointelegraph to DB')
    status = db.upsert_data2db(NewsArticles, df_cointelegraph, ['uniq_key'], col2update)
    print(f">> All News Articles inserted. \n>> {status}")
    time.sleep(2)

    print("Start Crawlers for Crypto.news ...")
    df_cryptonews = cr_task.crawl_cryptonews()
    print('>> Storing CryptoNews to DB')
    status = db.upsert_data2db(NewsArticles, df_cryptonews, ['uniq_key'], col2update)
    print(f">> All News Articles inserted. \n>> {status}")
    time.sleep(2)
    
    print("Start Crawlers for Investing.com ...")
    df_investingcom = cr_task.crawl_investingcom_news()
    print('>> Storing Investing.com News to DB')
    status = db.upsert_data2db(NewsArticles, df_investingcom, ['uniq_key'], col2update)
    print(f">> All News Articles inserted. \n>> {status}")
    time.sleep(2)

    print("Start Crawlers for u.today ...")
    df_utoday = cr_task.crawl_utoday_news()
    print('>> Storing U.Today News to DB')
    status = db.upsert_data2db(NewsArticles, df_utoday, ['uniq_key'], col2update)
    print(f">> All News Articles inserted. \n>> {status}")
    time.sleep(2)

    print("Start Crawlers for techcrunch ...")
    df_techcrunch = cr_task.crawl_techcrunch_news()
    print('>> Storing techcrunch news to DB')
    status = db.upsert_data2db(NewsArticles, df_techcrunch, ['uniq_key'], col2update)
    print(f">> All News Articles inserted. \n>> {status}")
    time.sleep(2)

    print("Start Crawlers for bitcoinist ...")
    df_bitcoinist = cr_task.crawl_bitcoinist_news()
    print('>> Storing bitcoinist news to DB')
    status = db.upsert_data2db(NewsArticles, df_bitcoinist, ['uniq_key'], col2update)
    print(f">> All News Articles inserted. \n>> {status}")
    time.sleep(2)


    # print("Start Crawlers for Unchained Crypto ...")
    # df_utoday = cr_task.crawl_unchainedcrypto_news()
    # print('>> Storing Unchained Crypto News to DB')
    # status_5 = insert_data2db(NewsArticles, df_utoday, ['uniq_key'])
    # print(f">> All News Articles inserted. \n>> {status_5}")
    # time.sleep(2)

@flow
def crawl_article_detail(rows=10):
    df = db.news_article_get_latest_link(rows)
    
    final_df = cr_task.get_article_details(df)[['uniq_key', 'full_content']]
    print(f"ROW-PULL-TOBE-PULL:{rows}, ROW-PULLED:{len(final_df)}")

    db.upsert_data2db(NewsArticles, final_df, ['uniq_key'], ['full_content'])

    return ''

