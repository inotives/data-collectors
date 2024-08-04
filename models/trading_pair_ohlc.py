from sqlalchemy import Column, Integer, String, Numeric, Date, DateTime
from models.base import Base

class TradingOHLC(Base):
    __tablename__ = 'trading_pair_ohlc'
    __table_args__ = (
        {'extend_existing': True}
    )  

    uniq_key = Column(String, primary_key=True) #uniq_key using source, timestamp, trade_pair, interval
    timestamp = Column(Integer, nullable=False) #in unix
    trade_date = Column(Date, nullable=False) # in data 
    trade_timestamp = Column(DateTime, nullable=False) # in timestamp
    source = Column(String, nullable=False) # source of data e.q: kraken, binance
    trade_pair = Column(String, nullable=False) # tradepair like XBTUSD, ETHUSD
    interval = Column(String, nullable=False) # interval=[1,5,15,30,60,720,1440]
    open = Column(Numeric, nullable=True) # opening price in the time period
    high = Column(Numeric, nullable=True) # highest price in the time period
    low = Column(Numeric, nullable=True) # lowest price in the time period
    close = Column(Numeric, nullable=True) # closing price in the time period
    volume = Column(Numeric, nullable=True) # total volume in the time period
    count = Column(Integer, nullable=True) # total trade count in the time period
    vwap = Column(Numeric, nullable=True) # Volume Weightage Average Price in the time period

