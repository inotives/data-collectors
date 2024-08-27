from sqlalchemy import Column, Integer, String, Numeric, Date, DateTime, BigInteger, Text
from models.base import Base

class CMCOHLCV(Base):
    __tablename__ = 'ohlcv_cmc'
    __table_args__ = (
        {'extend_existing': True}
    )

    uniq_key = Column(String, primary_key=True) # uniq_key using source, article_date, title
    metric_date = Column(DateTime)
    crypto = Column(String)
    open = Column(Numeric)
    high = Column(Numeric)
    low = Column(Numeric)
    close = Column(Numeric)
    volume = Column(Numeric)
    market_cap = Column(Numeric)
