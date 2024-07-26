from sqlalchemy import create_engine, Column, Integer, String, Sequence, MetaData
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session


Base = declarative_base()
metadata = MetaData()

class SqlAlc(object):
    def __init__(self, db_url):
        self.engine = create_engine(db_url)
        self.Session = scoped_session(sessionmaker(bind=self.engine))
        Base.metadata.bind = self.engine


    def execute_query(self, query, params=None):
        session = self.Session()
        try:
            result = session.execute(query, params)
            session.commit()
            if result.returns_rows:
                return result.fetchall()
        except SQLAlchemyError as e:
            session.rollback()
            print(f"Error executing query: {e}")
            raise
        finally:
            session.close()

    
    def create_table(self, table_class):
        try:
            table_class.__table__.create(self.engine, checkfirst=True)
        except SQLAlchemyError as e:
            print(f"Error creating table: {e}")
            raise


    def insert_data(self, table_class, data):
        session = self.Session()
        try:
            session.bulk_insert_mappings(table_class, data)
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            print(f"Error inserting data: {e}")
            raise
        finally:
            session.close()


    def close(self):
        self.Session.remove()
        
    