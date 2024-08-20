from prefect import task
import pandas as pd

from utils.web_crawlers import crawl_cointelegraph_tag, crawl_cryptonews_tag, crawl_investing_com, crawl_utoday

@task
def crawl_cointelegraph():
    
    tags = ['market', 'bitcoin', 'ethereum', 'altcoin', 'blockchain', 
            'business', 'regulation','ai','nft', 'defi']
    print('>>> Crawling Tags:', tags)
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
    print('>>> Crawling Tags:', tags)
    news_list = []
    for tag in tags:
        news_articles = crawl_cryptonews_tag(tag)
        news_list.append(news_articles)
    
    all_news_df = pd.concat(news_list, ignore_index=True).drop_duplicates(subset=['uniq_key'], keep='first')

    return all_news_df

@task
def crawl_investingcom_news():
    tags = ['cryptocurrency-news','commodities-news','stock-market-news',
            'forex-news', 'economy', 'economic-indicators', 'politics', 
            'world-news', 'company-news']
    print('>>> Crawling Tags:', tags)
    news_list = []
    for tag in tags: 
        news_articles = crawl_investing_com(tag)
        news_list.append(news_articles)
    
    all_news_df = pd.concat(news_list, ignore_index=True).drop_duplicates(subset=['uniq_key'], keep='first')

    return all_news_df


@task
def crawl_utoday_news():
    tags = ['bitcoin-news', 'ethereum-news', 'cardano-ada-coin-news', 'ripple-news', 
            'nft-news']
    print('>>> Crawling Tags:', tags)
    news_list = []
    for tag in tags: 
        news_articles = crawl_utoday(tag)
        news_list.append(news_articles)
    
    all_news_df = pd.concat(news_list, ignore_index=True).drop_duplicates(subset=['uniq_key'], keep='first')

    return all_news_df    