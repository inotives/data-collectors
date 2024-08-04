import psycopg2
from psycopg2 import sql, OperationalError, ProgrammingError, IntegrityError
from configs.settings import POSTGRES_DB_NAME, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_PWD, POSTGRES_USER

class Psql():
    def __init__(self): 
        self.host = POSTGRES_HOST
        self.user = POSTGRES_USER
        self.password = POSTGRES_PWD
        self.database = POSTGRES_DB_NAME
        self.port = POSTGRES_PORT
        self.connection = None 
    
    def connect(self):
        try: 
            conn_string = f"dbname='{self.database}' user='{self.user}' password='{self.password}' host='{self.host}' port='{self.port}'"
            self.connection = psycopg2.connect(conn_string)
        except OperationalError as e:
            print(f"Error connecting to the database: {e}")
            raise  
    
    # Function to execute query 
    def execute_query(self, query, params=None):
        if self.connection is None:
            raise Exception("Database connection is not established.")
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params)
                # Use cursor.description to determine if the query returns rows
                if cursor.description is not None:
                    # It's a SELECT query
                    return cursor.fetchall()
                else:
                    # Not a SELECT query, commit changes
                    self.connection.commit()
        except (ProgrammingError, IntegrityError) as e:
            print(f"Error executing query: {e}")
            self.connection.rollback()
            raise
        except Exception as e:
            print(f"Unexpected error: {e}")
            self.connection.rollback()
            raise    
    
    # Create table 
    def create_table(self, table_name, columns):
        column_defs = ', '.join([f"{name} {dtype}" for name, dtype in columns.items()])
        create_table_query = sql.SQL('''
            CREATE TABLE IF NOT EXISTS {table_name} (
                {column_defs}
            );
        ''').format(
            table_name=sql.Identifier(table_name),
            column_defs=sql.SQL(column_defs)
        )
        self.execute_query(create_table_query)

    # Normal Insert data functions
    def insert_data(self, table_name, data):
        if not data: return

        columns = data[0].keys()
        column_names = ', '.join(columns)
        placeholders = ', '.join([f"%({col})s" for col in columns])
        insert_query = sql.SQL('''
            INSERT INTO {table_name} ({column_names})
            VALUES ({placeholders});
        ''').format(
            table_name=sql.Identifier(table_name),
            column_names=sql.SQL(column_names),
            placeholders=sql.SQL(placeholders)
        )
        for row in data:
            self.execute_query(insert_query, row)
    

    # Insert and Update if primary key already exist 
    def upsert_data(self, table_name, data, conflict_columns):
        if not data: return

        columns = data[0].keys()
        column_names = ', '.join(columns)
        placeholders = ', '.join([f"%({col})s" for col in columns])
        conflict_clause = ', '.join([f"{col}" for col in conflict_columns])
        update_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col not in conflict_columns])

        upsert_query = sql.SQL('''
            INSERT INTO {table_name} ({column_names})
            VALUES ({placeholders})
            ON CONFLICT ({conflict_columns})
            DO UPDATE SET {update_clause};
        ''').format(
            table_name=sql.Identifier(table_name),
            column_names=sql.SQL(column_names),
            placeholders=sql.SQL(placeholders),
            conflict_columns=sql.SQL(conflict_clause),
            update_clause=sql.SQL(update_clause)
        )

        for row in data:
            self.execute_query(upsert_query, row)
    
    
    # Update data 
    def update_data(self, table_name, set_columns, where_conditions):
        set_clause = ', '.join([f"{col} = %({col})s" for col in set_columns.keys()])
        where_clause = ' AND '.join([f"{col} = %({col})s" for col in where_conditions.keys()])
        update_query = sql.SQL('''
            UPDATE {table_name}
            SET {set_clause}
            WHERE {where_clause};
        ''').format(
            table_name=sql.Identifier(table_name),
            set_clause=sql.SQL(set_clause),
            where_clause=sql.SQL(where_clause)
        )
        self.execute_query(update_query, {**set_columns, **where_conditions})
    
    
    # Delete data
    def delete_data(self, table_name, where_conditions):
        where_clause = ' AND '.join([f"{col} = %({col})s" for col in where_conditions.keys()])
        delete_query = sql.SQL('''
            DELETE FROM {table_name}
            WHERE {where_clause};
        ''').format(
            table_name=sql.Identifier(table_name),
            where_clause=sql.SQL(where_clause)
        )
        self.execute_query(delete_query, where_conditions)
    
    # Close connections 
    def close(self):
        if self.connection:
            self.connection.close()