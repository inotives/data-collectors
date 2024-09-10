from prefect import task, flow
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# custom
from configs.settings import POSTGRES_DB_URL
from models.base import SqlAlc

# Models 
from models.trading_pair_ohlc import TradingOHLC
from models.coinmetric_data import CoinmetricDaily
from models.ethereum_token_txes import EthTokenTxes
from models.news_articles import NewsArticles
from models.cmc_ohlcv import CMCOHLCV
from models.fiat_sgd_rates import FiatSGDRates

''' -- TASKS ------------------------------------------------------------- '''
@task 
def create_table(table_class):    
    conn = SqlAlc(POSTGRES_DB_URL)
    
    status = conn.create_table(table_class)
    print(status)

    conn.close()
    
@task 
def insert_data2db(table_class, data, conflicts): 
    conn = SqlAlc(POSTGRES_DB_URL)
    
    status = conn.upsert_data(table_class, data, conflicts)
    print(status)

    conn.close()

@task
def upsert_data2db(table_class, data, conflicts, col2update):
    """
    Upsert dataframe to db
    """
    conn = SqlAlc(POSTGRES_DB_URL)

    status = conn.update_col_data(table_class, data, conflicts, col2update)
    print(status)
    
    conn.close()

@task
def export_data2csv(table_class, output_file='output', filters=None, date_field=None, date_range=None):
    """
    Exporting data from DB to csv
    """
    conn = SqlAlc(POSTGRES_DB_URL)

    status = conn.export_to_csv(
        table_class, 
        output_file=output_file, 
        filters=filters,
        date_range=date_range,
        date_field=date_field
    )

    return status

def insert_dataframe_to_postgres(df: pd.DataFrame, table_name: str):
    """
    Inserts DataFrame data into a PostgreSQL table.
    
    Parameters:
        df (pd.DataFrame): The DataFrame to insert into the PostgreSQL table.
        table_name (str): The name of the table to insert data into.
        db_params (dict): Database connection parameters.
    """
    # Create a SQLAlchemy engine for PostgreSQL
    engine = create_engine(POSTGRES_DB_URL)
    
    # Define SQL command to create the table if it does not exist
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
    {', '.join([f'{col} VARCHAR(255)' for col in df.columns])}
    );
    """

    try:
        with engine.connect() as connection:
            # Execute the SQL command to create the table
            connection.execute(text(create_table_sql))
            
            # Insert DataFrame into PostgreSQL table
            df.to_sql(table_name, engine, if_exists='append', index=False)
        
        print(f"Data inserted into table '{table_name}' successfully.")

    except SQLAlchemyError as e:
        print(f"Error: {e}")
    

    
    
     
''' -- FLOWS ------------------------------------------------------------- '''
@flow
def init_db():
    create_table(TradingOHLC)
    create_table(CoinmetricDaily)
    create_table(EthTokenTxes)
    create_table(NewsArticles)
    create_table(CMCOHLCV)
    create_table(FiatSGDRates)
    print(">>> All tables successfully created!!")


""" -- DB functions -------------------------------------------------------- """
def etherscan_tokentx_get_latest_blocknum(asset=None):
    """get etherscan token tx latest block num"""
    conn = SqlAlc(POSTGRES_DB_URL)

    where_clause = f"WHERE ett.token_symbol = '{asset}'" if asset else ''
    query = f"""SELECT MAX(ett.block_number) AS bknum
    FROM ethereum_token_txes ett
    {where_clause}
    """
    latest_block = conn.execute_query(query)

    return latest_block[0][0]

def news_article_get_latest_link(rows=10):
    """get latest news article link in db """
    engine = create_engine(POSTGRES_DB_URL)

    query = f"""
    SELECT na.uniq_key, na.link, na.full_content 
    FROM news_articles na 
    WHERE na.full_content IS NULL
    ORDER BY na.article_date ASC
    LIMIT {rows};
    """

    with engine.connect() as conn:
        df = pd.read_sql(query, conn)

    return df 
    



