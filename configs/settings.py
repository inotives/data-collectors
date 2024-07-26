POSTGRES_USER = 'inotives' # make sure to change this to block
POSTGRES_PWD = 'postgrespwd' # make sure to change this to block
POSTGRES_PORT = '5433' 
POSTGRES_DB_NAME = 'inotives'
POSTGRES_HOST = 'localhost'
POSTGRES_DB_URL = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PWD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB_NAME}"
