from prefect import task
import pandas as pd

from utils.web_crawlers import crawl_cointelegraph_tag, crawl_cryptonews_tag

@task
def crawl_cointelegraph():

    tags = ['market', 'bitcoin', 'ethereum', 'altcoin', 'blockchain', 
            'business', 'regulation','ai','nft', 'defi']

    news_list = []
    for tag in tags: 
        news_articles = crawl_cointelegraph_tag(tag)
        news_list.append(news_articles)

    all_news_df = pd.concat(news_list, ignore_index=True).drop_duplicates(subset=['uniq_key'], keep='first')

    return all_news_df
    
@task
def crawl_cryptonews():
    tags = ['blockchain', 'bitcoin', 'ethereum', 'defi', 'altcoin', 
            'regulation', 'nft', 'metaverse']
    news_list = []
    for tag in tags:
        news_articles = crawl_cryptonews_tag(tag)
        news_list.append(news_articles)
    
    all_news_df = pd.concat(news_list, ignore_index=True).drop_duplicates(subset=['uniq_key'], keep='first')

    return all_news_df