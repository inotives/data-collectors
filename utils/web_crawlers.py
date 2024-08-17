import requests
from requests_html import HTMLSession

from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta

from utils.tools import export_csv_to_data


def cointelegraph_crawler():
    # Define the URL
    url = 'https://cointelegraph.com/'

    # Set up the headers with a User-Agent
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    # Send a GET request to fetch the raw HTML content
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Define the current timestamp
    current_time = datetime.now()

    # Lists to store extracted data
    titles = []
    timestamps = []
    posters = []
    dates = []

    # Find all article tags
    articles = soup.find_all('article', class_='post-card__article')
    
    # Hourly timestamp
    capture_at_hour = datetime.now().strftime('%Y-%m-%d %H:00:00')

    # Extract data from each article
    for article in articles:
        title_tag = article.find('span', class_='post-card__title')
        timestamp_tag = article.find('time', class_='text-uiSWeak')
        poster_tag = article.find('a', class_='post-card__author-link')
        
        if title_tag and timestamp_tag and poster_tag:
            title = title_tag.get_text(strip=True)
            poster = poster_tag.get_text(strip=True)
            date_attr = article.find('time')['datetime']
            date = datetime.strptime(date_attr, '%Y-%m-%d')  # Extract the date from datetime attribute
            
            # Extract and convert the relative time
            time_text = timestamp_tag.get_text(strip=True)
            if 'minute' in time_text:
                minutes = int(time_text.split()[0])
                timestamp = current_time - timedelta(minutes=minutes)
            elif 'hour' in time_text:
                hours = int(time_text.split()[0])
                timestamp = current_time - timedelta(hours=hours)
            else:
                timestamp = current_time  # Default to current time if unknown format

            # Append data to lists
            titles.append(title)
            timestamps.append(timestamp)
            posters.append(poster)
            dates.append(date)

    # Create a DataFrame from the extracted data
    df = pd.DataFrame({
        'Title': titles,
        'Timestamp': timestamps,
        'Poster': posters,
        'Date': dates
    })

    export_csv_to_data(df, f"cointelegraph_news_{capture_at_hour}")


def cointelegraph_market():
    
    #url = 'https://cointelegraph.com/tags/markets'
    url = 'https://cointelegraph.com/tags/bitcoin'
    
    # Set up the headers with a User-Agent
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    # Send a GET request to fetch the raw HTML content
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')

    articles = soup.find_all('article', class_='post-card-inline')

    # Lists to store extracted data
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
        author = article.find('p', class_='post-card-inline__author').get_text(strip=True).replace('by ', '')
        
        # Extract the posted datetime
        art_date = article.find('time', class_='post-card-inline__date')['datetime']

        # Extract the article URL
        article_url = article.find('a', class_='post-card-inline__title-link')['href']
        full_url = f"https://cointelegraph.com{article_url}"

        titles.append(title)
        contents.append(content)
        posters.append(author)
        dates.append(art_date)
        links.append(full_url)
    
    # Create dataframe from the result 
    df = pd.DataFrame({
        'title': titles,
        'contents': contents,
        'article': art_date,
        'author': posters,
        'link': links
    })

    print(df)

    capture_at_hour = datetime.now().strftime('%Y%m%d-%H')

    export_csv_to_data(df, f"cointelegraph_bitcoin_{capture_at_hour}")


    
