import requests
import time
import re

from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta

from utils.tools import generate_unique_key

def crawl_site(url): 
    
    # Set up the headers with a User-Agent
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    # Send a GET request to fetch the raw HTML content
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')

    return soup

def crawl_site_html_text(url):

    # Set up the headers with a User-Agent
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    # Send a GET request to fetch the raw HTML content
    response = requests.get(url, headers=headers)

    if not response.ok:
        print('Status code:', response.status_code)
        raise Exception('Failed to load page {}'.format(url))

    page_content = response.text
    soup = BeautifulSoup(page_content, 'html.parser')

    return soup

def articles_fields():
    return {
        'uniq_keys': [],
        'titles': [],
        'contents': [],
        'authors': [],
        'dates': [],
        'links': []
    }

def crawl_cointelegraph_tag(tag):
    source_site = 'cointelegraph'
    url = f"https://cointelegraph.com/tags/{tag}"
    soup = crawl_site(url)

    # Find article tags with its class
    articles = soup.find_all('article', class_='post-card-inline')

    # Lists to store extracted data
    fields = articles_fields()

    for article in articles: 
        # Extract the title
        title = article.find('span', class_='post-card-inline__title').get_text(strip=True)
        
        # Extract the content/summary
        content = article.find('p', class_='post-card-inline__text').get_text(strip=True)
        
        # Extract the author
        author = article.find('p', class_='post-card-inline__author').get_text(strip=True).replace('by', '')
        
        # Extract the posted datetime
        art_date = article.find('time', class_='post-card-inline__date')['datetime']

        # Extract the article URL
        article_url = article.find('a', class_='post-card-inline__title-link')['href']
        full_url = f"https://cointelegraph.com{article_url}"
        
        # Generate uniq key 
        uniq_key = generate_unique_key(title, source_site , author)

        # Append the result to lists
        fields['uniq_keys'].append(uniq_key)
        fields['titles'].append(title)
        fields['contents'].append(content)
        fields['authors'].append(author)
        fields['dates'].append(art_date)
        fields['links'].append(full_url)
    
    # Timestamp to check when these data was capture
    capture_at = datetime.now().strftime('%Y-%m-%d %H:%M:00')

    # Create dataframe from the result 
    df = pd.DataFrame({
        'uniq_key': fields['uniq_keys'],
        'title': fields['titles'],
        'source': source_site,
        'content': fields['contents'],
        'article_date': fields['dates'],
        'author': fields['authors'],
        'link': fields['links'],
        'tag': tag,
        'captured_at': capture_at
    })

    time.sleep(2) # sleep for 2 sec. to avoid been too agresive call to the site. 
    print(f">>>> Complete Crawling Tag: {tag}, Return Result: {len(df)} Articles")
    return df


def crawl_cryptonews_tag(tag):
    source_site = 'crypto.news'
    url = f"https://crypto.news/tag/{tag}/"
    soup = crawl_site(url)

    # Find article tags with its class
    articles = soup.find_all('div', class_='post-loop__content')

    # List to hold extracted article data
    fields = articles_fields()

    for article in articles:

        # extract title 
        title_tag = article.find('p', class_='post-loop__title').find('a')
        title = title_tag.text.strip()

        # Extract the link
        link = title_tag['href']

        # Extract the content/summary
        content = article.find('div', class_='post-loop__summary').text.strip()

        author = 'No Author'

        # Extract the timestamp
        timestamp_tag = article.find('time', class_='post-loop__date')
        timestamp = datetime.fromisoformat(timestamp_tag['datetime'])

        uniq_key = generate_unique_key(title, source_site)

        # Append the result to lists
        fields['uniq_keys'].append(uniq_key)
        fields['titles'].append(title)
        fields['contents'].append(content)
        fields['authors'].append(author)
        fields['dates'].append(timestamp.date())
        fields['links'].append(link)
        
    # Timestamp to check when these data was capture
    capture_at = datetime.now().strftime('%Y-%m-%d %H:%M:00')

    # Create dataframe from the result 
    df = pd.DataFrame({
        'uniq_key': fields['uniq_keys'],
        'title': fields['titles'],
        'source': source_site,
        'content': fields['contents'],
        'article_date': fields['dates'],
        'author': fields['authors'],
        'link': fields['links'],
        'tag': tag,
        'captured_at': capture_at
    })

    time.sleep(2) # sleep for 2 sec. to avoid been too agresive call to the site. 
    print(f">>>> Complete Crawling Tag: {tag}, Return Result: {len(df)} Articles")
    return df


def crawl_investing_com(tag):
    source = 'investing.com'
    capture_at = datetime.now().strftime('%Y-%m-%d %H:%M:00')

    url = f"https://www.investing.com/news/{tag}"
    soup = crawl_site(url)

    # Find article tags with its class
    articles = soup.find_all('article', class_='news-analysis-v2_article__wW0pT')

    fields = articles_fields()

    for article in articles:
        # Extract title
        title_tag = article.find('a', class_='text-inv-blue-500')
        title = title_tag.text.strip() if title_tag else 'No title found'
        
        # Extract content (description)
        content_tag = article.find('p', class_='overflow-hidden')
        content = content_tag.text.strip() if content_tag else 'No content found'
        
        # Extract author (by who)
        author_tag = article.find('span', {'data-test': 'news-provider-name'})
        author = author_tag.text.strip() if author_tag else 'No author found'
        
        # Extract post timestamp
        time_tag = article.find('time', {'data-test': 'article-publish-date'})
        post_timestamp = time_tag['datetime'].split()[0] if time_tag else None
        
        # Extract link of the article
        link = title_tag['href'] if title_tag and title_tag.has_attr('href') else 'No link found'
        
        uniq_key = generate_unique_key(title, source)

        fields['uniq_keys'].append(uniq_key)
        fields['titles'].append(title)
        fields['contents'].append(content)
        fields['authors'].append(author)
        fields['dates'].append(post_timestamp)
        fields['links'].append(link)
    
    # Timestamp to check when these data was capture
    capture_at = datetime.now().strftime('%Y-%m-%d %H:%M:00')

    df = pd.DataFrame({
        'uniq_key': fields['uniq_keys'],
        'title': fields['titles'],
        'source': source,
        'content': fields['contents'],
        'article_date': fields['dates'],
        'author': fields['authors'],
        'link': fields['links'],
        'tag': tag,
        'captured_at': capture_at
    })

    time.sleep(2) # sleep for 2 sec. to avoid been too agresive call to the site. 
    print(f">>>> Complete Crawling Tag: {tag}, Return Result: {len(df)} Articles")
    return df


def crawl_utoday(tag):
    source = 'u.today'
    capture_at = datetime.now().strftime('%Y-%m-%d %H:%M:00')

    url = f"https://u.today/{tag}"
    soup = crawl_site(url)

    # Find article tags with its class
    articles = soup.find_all('div', class_='news__item')

    fields = articles_fields()
    
    for article in articles:
        # Extract the post date
        # Extract and convert the post date to a date object
        post_date_str = article.find('div', class_='humble').get_text(strip=True)
        post_date = datetime.strptime(post_date_str.split('-')[0].strip(), '%b %d, %Y').date()

        # Extract the title
        title = article.find('div', class_='news__item-title').get_text(strip=True)

        # Extract the content (since the content isn't explicitly provided, we will skip it)
        content = ''

        # Extract the author
        author = article.find('a', class_='humble--author').get_text(strip=True)

        # Extract the link
        link = article.find('a', class_='news__item-body')['href']
        full_link = f"https://u.today{link}" if not link.startswith('http') else link

        uniq_key = generate_unique_key(title, source)

        fields['uniq_keys'].append(uniq_key)
        fields['titles'].append(title)
        fields['contents'].append(content)
        fields['authors'].append(author)
        fields['dates'].append(post_date)
        fields['links'].append(full_link)
    
    df = pd.DataFrame({
        'uniq_key': fields['uniq_keys'],
        'title': fields['titles'],
        'source': source,
        'content': fields['contents'],
        'article_date': fields['dates'],
        'author': fields['authors'],
        'link': fields['links'],
        'tag': tag,
        'captured_at': capture_at
    })

    time.sleep(2) # sleep for 2 sec. to avoid been too agresive call to the site. 
    print(f">>>> Complete Crawling Tag: {tag}, Return Result: {len(df)} Articles")

    return df 


def crawl_unchainedcrypto(tag=None):
    source = 'unchainedcrypto'
    capture_at = datetime.now().strftime('%Y-%m-%d %H:%M:00')

    url = f"https://unchainedcrypto.com/news/" # crawl the latest news
    soup = crawl_site(url)

    # Find article tags with its class
    articles = soup.find_all('div', class_='group-day')

    fields = articles_fields()
    fields['tags'] = []
    
    for article in articles:

        tag = article.find('p', class_='cat-group').text.strip()
        # Extract the post link from the <a> tag inside <div class="archive-item">
        post_link = article.find('a')['href']
        
        # Extract the title from the <h3> tag inside <div class="title">
        title = article.find('h3').text.strip()

        # Extract the content from the <p> tag inside <div class="post-teaser">
        content = article.find('div', class_='post-teaser').find('p').text.strip()

        # Extract the post date from the <div class="episode"> and convert it to a date object
        post_date_str = article.find('div', class_='episode').text.strip()
        post_date = datetime.strptime(post_date_str, '%B %d, %Y').date()

        # Author is not explicitly provided in the structure, so set it to None
        author = ''
        
        uniq_key = generate_unique_key(title, source)

        fields['uniq_keys'].append(uniq_key)
        fields['titles'].append(title)
        fields['contents'].append(content)
        fields['authors'].append(author)
        fields['dates'].append(post_date)
        fields['links'].append(post_link)
        fields['tags'].append(tag)
    
    df = pd.DataFrame({
        'uniq_key': fields['uniq_keys'],
        'title': fields['titles'],
        'source': source,
        'content': fields['contents'],
        'article_date': fields['dates'],
        'author': fields['authors'],
        'link': fields['links'],
        'tag': fields['tags'],
        'captured_at': capture_at
    })

    time.sleep(2) # sleep for 2 sec. to avoid been too agresive call to the site. 
    print(f">>>> Complete Crawling Tag: Latest News, Return Result: {len(df)} Articles")

    return df


def crawl_sgrates(currency_date, source_bank='dbs'):
    """ Crawl the SGD rates of singapore Banks"""
    url = f"https://www.sgrates.com/bankrate/{source_bank}.html?date={currency_date}"
    
    soup = crawl_site_html_text(url)
    table = soup.find('table')

    # Initialize lists to store the data
    currencies = []
    bank_buy_tt = []
    bank_sell_tt = []
    bank_buy_od = []
    bank_sell_od = []
    uniq_keys = []

    # Loop through each row in the table, excluding the header
    for row in table.find_all('tr')[1:]:
        cols = row.find_all('td')
        currency = cols[0].text.strip()
        uniq_key = generate_unique_key(currency_date, currency, source_bank)

        # Function to safely convert to float or return None
        def safe_float(value):
            try:
                return float(value)
            except ValueError:
                return None

        # Convert to float, replace '--' with None and coerce to NaN
        buy_tt = safe_float(cols[1].text.strip())
        sell_tt = safe_float(cols[2].text.strip())
        buy_od = safe_float(cols[3].text.strip())
        sell_od = safe_float(cols[4].text.strip())

        # Append the data to the lists
        uniq_keys.append(uniq_key)
        currencies.append(currency)
        bank_buy_tt.append(buy_tt)
        bank_sell_tt.append(sell_tt)
        bank_buy_od.append(buy_od)
        bank_sell_od.append(sell_od)

    # Create a DataFrame
    df = pd.DataFrame({
        'uniq_key': uniq_keys,
        'tx_date': currency_date,
        'source_bank': source_bank,
        'currency': currencies,
        'bank_buy_tt': bank_buy_tt,
        'bank_sell_tt': bank_sell_tt,
        'bank_buy_od': bank_buy_od,
        'bank_sell_od': bank_sell_od
    })

    return df

def crawl_article_details(url):
    '''Scrap the article details '''
    
    soup = crawl_site_html_text(url)

    # Find article tags with its class
    articles_text = soup.find_all("p")
    full_text = ''

    if articles_text:
        # Find all paragraphs within the article
        paragraphs = articles_text
        # Collect all the paragraph texts
        full_text = "\n".join([p.get_text() for p in paragraphs])
        
    else:
        print("Article content not found.")
    
    time.sleep(1)

    return full_text


def crawl_test():
    source = 'cointelegraph'
    capture_at = datetime.now().strftime('%Y-%m-%d %H:%M:00')

    url = f"https://u.today/crypto-market-bloodbath-explanation-provided-by-jim-cramer"
    soup = crawl_site_html_text(url)

    # Find article tags with its class
    articles_text = soup.find_all("p")
    full_text = ''

    if articles_text:
        # Find all paragraphs within the article
        paragraphs = articles_text
        # Collect all the paragraph texts
        full_text = " ".join([p.get_text() for p in paragraphs])
        
        # Print the concatenated text
        print(full_text)
    else:
        print("Article content not found.")

    return full_text