import requests
from requests_html import HTMLSession

from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta

from utils.tools import export_csv_to_data, generate_unique_key

def crawl_site(url): 
    
    # Set up the headers with a User-Agent
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    # Send a GET request to fetch the raw HTML content
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')

    return soup


def crawl_cointelegraph_tag(tag):
    source_site = 'cointelegraph'
    url = f"https://cointelegraph.com/tags/{tag}"
    soup = crawl_site(url)

    # Find article tags with its class
    articles = soup.find_all('article', class_='post-card-inline')

    # Lists to store extracted data
    uniq_keys = []
    titles = []
    contents = []
    posters = []
    dates = []
    links = []

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
        uniq_key = generate_unique_key(title, author)

        # Append the result to lists
        uniq_keys.append(uniq_key)
        titles.append(title)
        contents.append(content)
        posters.append(author)
        dates.append(art_date)
        links.append(full_url)
    
    # Timestamp to check when these data was capture
    capture_at = datetime.now().strftime('%Y-%m-%d %H:%M:00')

    # Create dataframe from the result 
    df = pd.DataFrame({
        'uniq_key': uniq_keys,
        'title': titles,
        'source': source_site,
        'content': contents,
        'article_date': art_date,
        'author': posters,
        'link': links,
        'captured_at': capture_at
    })

    return df

def crawl_cryptonews_tag(tag):
    source_site = 'crypto.news'
    url = f"https://crypto.news/tag/{tag}/"
    soup = crawl_site(url)

    # Find article tags with its class
    articles = soup.find_all('div', class_='post-loop__content')

    # List to hold extracted article data
    uniq_keys = []
    titles = []
    contents = []
    posters = []
    dates = []
    links = []

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

        uniq_key = generate_unique_key(title)

        # Append the result to lists
        uniq_keys.append(uniq_key)
        titles.append(title)
        contents.append(content)
        posters.append(author)
        dates.append(timestamp.date())
        links.append(link)
        
    # Timestamp to check when these data was capture
    capture_at = datetime.now().strftime('%Y-%m-%d %H:%M:00')

    # Create dataframe from the result 
    df = pd.DataFrame({
        'uniq_key': uniq_keys,
        'title': titles,
        'source': source_site,
        'content': contents,
        'article_date': dates,
        'author': posters,
        'link': links,
        'captured_at': capture_at
    })

    return df