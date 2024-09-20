from prefect import task
import pandas as pd

import utils.web_crawlers as wc

@task
def crawl_cointelegraph():
    
    tags = ['market', 'bitcoin', 'ethereum', 'altcoin', 'blockchain', 
            'business', 'regulation','ai','nft', 'defi', 'research-articles']
    print('>>> Crawling Tags:', tags)
    news_list = []
    for tag in tags: 
        news_articles = wc.crawl_cointelegraph_tag(tag)
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
        news_articles = wc.crawl_cryptonews_tag(tag)
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
        news_articles = wc.crawl_investing_com(tag)
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
        news_articles = wc.crawl_utoday(tag)
        news_list.append(news_articles)
    
    all_news_df = pd.concat(news_list, ignore_index=True).drop_duplicates(subset=['uniq_key'], keep='first')

    return all_news_df    

@task
def crawl_unchainedcrypto_news():
    print(">>> Start Scraping Latest News from Unchained Crypto")
    all_news_df = wc.crawl_unchainedcrypto()

    return all_news_df


@task
def crawl_techcrunch_news(page=None):
    categories = ['cryptocurrency', 'gaming', 'venture', 'artificial-intelligence', 
                  'startups', 'fintech', 'fundraising', 'enterprise']
    print(">>> Start Scraping Latest News from Techcrunch")
    print('>>> Crawling Tags:', categories)
    news_list = []
    for catg in categories:
        news_articles = wc.crawl_techcrunch(catg)
        news_list.append(news_articles)

    all_news_df = pd.concat(news_list, ignore_index=True).drop_duplicates(subset=['uniq_key'], keep='first')

    return all_news_df

@task
def crawl_bitcoinist_news():
    categories = [ 'bitcoin', 'bitcoin-price', 'blockchain-technology', 'industry', 
        'altcoin-price', 'altcoin-news', 'ethereum', 'ripple', 'litecoin', 'eos'
    ]
    print(">>> Start Scraping Latest News from Bitcoinist")
    print('>>> Crawling Tags:', categories)
    news_list = []
    for catg in categories:
        news_articles = wc.crawl_bitcoinist(catg)
        news_list.append(news_articles)

    all_news_df = pd.concat(news_list, ignore_index=True).drop_duplicates(subset=['uniq_key'], keep='first')

    return all_news_df


@task
def crawl_coineagle_news():
    categories = ['crypto', 'bitcoin', 'ethereum']
    print(">>> Start Scraping Latest News from Coineagle")
    print('>>> Crawling Tags:', categories)
    news_list = []
    for catg in categories:
        news_articles = wc.crawl_coineagle(catg)
        news_list.append(news_articles)

    all_news_df = pd.concat(news_list, ignore_index=True).drop_duplicates(subset=['uniq_key'], keep='first')

    return all_news_df


@task
def crawl_fxempire_news():
    categories = ['cryptocurrencies-news', 'forex-news', 'economic-news', 'stocks-news']
    print(">>> Start Scraping Latest News from FXempire")
    print('>>> Crawling Tags:', categories)
    news_list = []
    for catg in categories:
        news_articles = wc.crawl_fxempire(catg)
        news_list.append(news_articles)

    all_news_df = pd.concat(news_list, ignore_index=True).drop_duplicates(subset=['uniq_key'], keep='first')
    df_cleaned = all_news_df.dropna(subset=['title'])

    return df_cleaned


@task
def get_article_details(article_df):

    article_df['full_content'] = article_df['link'].apply(wc.crawl_article_details)

    return article_df
    # wc.crawl_article_details(article_url)

