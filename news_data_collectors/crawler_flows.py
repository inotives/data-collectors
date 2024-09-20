import time
from prefect import flow 

import news_data_collectors.crawler_tasks as cr_task
from models.news_articles import NewsArticles
import utils.db as db

def crawl_site(site_name, crawler_func, col2update):
    sleep_time = 2
    try: 
        print(f">> Crawling for {site_name} News ... ...")
        df_news = crawler_func
        print(f">> Start Storing {site_name} news to DB. Total Articles:{len(df_news)}")
        status = db.upsert_data2db(NewsArticles, df_news, ['uniq_key'], col2update)
        print(f">> All News Articles inserted. \n>> {status}")
        time.sleep(sleep_time)
    except AttributeError as e:
        print(f"Attribute Error:: {e}")
    except Exception as e: 
        print(f"ERROR OCCURED: {e}")


@flow
def crawl_news():
    col2update = ['uniq_key', 'source', 'title', 'content', 'link', 'article_date', 'author', 'tag', 'captured_at']
    # Crawlers 

    crawl_site('CoinTelegraph', cr_task.crawl_cointelegraph(), col2update)
    crawl_site('Investing.com', cr_task.crawl_investingcom_news(), col2update)
    crawl_site('Crypto.news', cr_task.crawl_cryptonews(), col2update)
    crawl_site('u.today', cr_task.crawl_utoday_news(), col2update)
    crawl_site('techcrunch', cr_task.crawl_techcrunch_news(), col2update)
    crawl_site('bitcoinist', cr_task.crawl_bitcoinist_news(), col2update)
    crawl_site('coineagle', cr_task.crawl_coineagle_news(), col2update)
    crawl_site('FXempire', cr_task.crawl_fxempire_news(), col2update)
    
    # crawl_site('Unchained Crypto', cr_task.crawl_unchainedcrypto_news(), col2update)
    
    

@flow
def crawl_article_detail(rows=10):
    df = db.news_article_get_latest_link(rows)
    
    final_df = cr_task.get_article_details(df)[['uniq_key', 'full_content']]
    print(f"ROW-PULL-TOBE-PULL:{rows}, ROW-PULLED:{len(final_df)}")

    db.upsert_data2db(NewsArticles, final_df, ['uniq_key'], ['full_content'])

    return ''

