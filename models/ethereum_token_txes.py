from sqlalchemy import Column, Integer, String, Numeric, Date, DateTime, BigInteger
from models.base import Base

class EthTokenTxes(Base):
    __tablename__ = 'ethereum_token_txes'
    __table_args__ = (
        {'extend_existing': True}
    )  

    uniq_key = Column(String, primary_key=True) #uniq_key using source, timestamp, trade_pair, interval
    block_number = Column(Integer, nullable=False) #in int
    timestamp = Column(Numeric, nullable=False) #in unix
    transaction_date = Column(Date, nullable=False)
    transaction_timestamp = Column(DateTime, nullable=False) 
    transaction_hash = Column(String, nullable=False) 
    transaction_value = Column(Numeric)
    transaction_index = Column(Integer)
    nonce = Column(Integer, nullable=True)
    block_hash = Column(String, nullable=False)
    contract_addr = Column(String, nullable=False)
    from_addr = Column(String, nullable=False)
    to_addr = Column(String, nullable=False)
    token_name = Column(String)
    token_symbol = Column(String)
    token_decimal = Column(Integer)
    gas = Column(Numeric)
    gas_price = Column(Numeric)
    gas_used = Column(Numeric)
    cumulative_gas_used = Column(Numeric)
    transaction_input = Column(String)
    confirmations = Column(Integer)
