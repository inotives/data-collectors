import sys
from utils.db import export_data2csv

from models.news_articles import NewsArticles

if __name__ == '__main__':
    script_name = sys.argv[1]

    if script_name=='csv-export':
        start_date = '2024-08-01'
        end_date = '2024-08-18'
        export_data2csv(
            NewsArticles, 
            filters={'source': 'crypto.news'},
            date_field='article_date',
            date_range=(start_date, end_date),
            output_file=f"NEWS_{start_date.replace('-','')}_{end_date.replace('-','')}"
        )
    else: 
        print("Not a valid script ...")