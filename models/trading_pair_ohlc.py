from sqlalchemy import Column, Integer, String, ARRAY, Text, Numeric, Date, DateTime
from models.base import Base

class TradingOHLC(Base):
    __tablename__ = 'trading_pair_ohlc'
    __table_args__ = (
        {'extend_existing': True}
    )  

    uniq_key = Column(String, primary_key=True) #uniq_key using source, timestamp, trade_pair, interval
    timestamp = Column(Integer, nullable=False)
    trade_date = Column(Date, nullable=False)
    trade_timestamp = Column(DateTime, nullable=False)
    source = Column(String, nullable=False)
    trade_pair = Column(String, nullable=False)
    interval = Column(String, nullable=False) # interval=[1m,5m,15m,30m,1h,12h,24h]
    open = Column(Numeric, nullable=True)
    high = Column(Numeric, nullable=True)
    low = Column(Numeric, nullable=True)
    close = Column(Numeric, nullable=True)
    volume = Column(Numeric, nullable=True)
    count = Column(Integer, nullable=True)

