import pandas as pd
from sqlalchemy import create_engine, MetaData, sql
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.pool import QueuePool

from utils.tools import export_csv_to_data

Base = declarative_base()
metadata = MetaData()

class SqlAlc(object):
    def __init__(self, db_url):
        self.engine = create_engine(
            db_url,
            poolclass=QueuePool,
            pool_size=10,
            max_overflow=20,
            pool_timeout=30,  # wait for 30 seconds before giving up on getting a connection from the pool
            connect_args={
                "options": "-c statement_timeout=300000"  # 5 minutes
            }
        )
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
            return {"status": "Success", "message": f"Table '{table_class.__tablename__}' created or already exists."}
    
        except SQLAlchemyError as e:
            print(f"Error creating table: {e}")
            return {"status": "Failed", "error": str(e)}


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

    def upsert_data(self, table_class, df, conflict_columns, batch_size=500):
        if df.empty:
            return {'status': 'No data to upsert', "rows_upserted": 0}
        
        data_dicts = df.to_dict(orient='records')
        session = self.Session()
        rows_upserted = 0

        try:
            for i in range(0, len(data_dicts), batch_size):
                batch = data_dicts[i:i + batch_size]
                stmt = insert(table_class).values(batch)
                update_dict = {c.name: c for c in stmt.excluded if c.name not in conflict_columns}
                on_conflict_stmt = stmt.on_conflict_do_update(
                    index_elements=conflict_columns,
                    set_=update_dict
                )
                result = session.execute(on_conflict_stmt)
                rows_upserted += result.rowcount

            session.commit()
            return {"status": "Success", "rows_upserted": rows_upserted}
        
        except SQLAlchemyError as e:
            session.rollback()
            print(f"Error upserting data: {e}")
            raise

        finally:
            session.close()
    

    def export_to_csv(self, table_class, filters=None, date_field=None, date_range=None, output_file='output'):
        session = self.Session()
        try:
            # Build the query with optional filters
            query = session.query(table_class)
            if filters:
                for field, value in filters.items():
                    query = query.filter(getattr(table_class, field) == value)

            if date_range and date_field:
                start_date, end_date = date_range
                query = query.filter(getattr(table_class, date_field).between(start_date, end_date))

            # Convert the query to a DataFrame
            df = pd.read_sql(query.statement, self.engine)
            
            # Export to CSV
            export_csv_to_data(data=df, filename=output_file)

            return {"status": "Success", "message": f"Data exported to {output_file}"}
        
        except SQLAlchemyError as e:
            print(f"Error exporting data: {e}")
            return {"status": "Failed", "error": str(e)}
        
        finally:
            session.close()


    def close(self):
        self.Session.remove()
        
    