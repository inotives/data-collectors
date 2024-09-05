from sqlalchemy import Column, Integer, String, Numeric, Date, DateTime, BigInteger, Text
from models.base import Base

class NewsArticles(Base):
    __tablename__ = 'news_articles'
    __table_args__ = (
        {'extend_existing': True}
    )

    uniq_key = Column(String, primary_key=True) # uniq_key using source, article_date, title
    source = Column(String)
    title = Column(Text)
    content = Column(Text)
    full_content = Column(Text)
    link = Column(Text)
    article_date = Column(Date)
    author = Column(String)
    tag = Column(String)
    sentiment_label = Column(String)
    captured_at = Column(DateTime)
