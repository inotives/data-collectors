from sqlalchemy import Column, Integer, String, Numeric, Date, DateTime, BigInteger, Text
from models.base import Base

class FiatSGDRates(Base):
    __tablename__ = 'fiat_sgd_rates'
    __table_args__ = (
        {'extend_existing': True}
    )

    uniq_key = Column(String, primary_key=True) # uniq_key using source, article_date, title
    source_bank = Column(String)
    tx_date = Column(DateTime)
    currency = Column(String)
    bank_buy_tt = Column(Numeric)
    bank_sell_tt = Column(Numeric)
    bank_buy_od = Column(Numeric)
    bank_sell_od = Column(Numeric)
