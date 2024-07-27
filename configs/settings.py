from dotenv import dotenv_values

config = dotenv_values('.env')

POSTGRES_USER = config['POSTGRES_USER'] 
POSTGRES_PWD = config['POSTGRES_PWD']
POSTGRES_PORT = '5433' 
POSTGRES_DB_NAME = 'inotives'
POSTGRES_HOST = 'localhost'
POSTGRES_DB_URL = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PWD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB_NAME}"


print(f'{POSTGRES_USER}, {POSTGRES_PWD}')