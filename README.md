# data-collectors 

This project aims to create a simple data pipeline using Prefect to collect data from sources like REST API, FIX, WebSocket, GraphQL, and data crawlers. All collected data will be stored in PostgreSQL for further analysis and used by a prediction engine to predict price trends for trading bots. These sources include various free public data such as open-exchange-rates, Etherscan, crypto-exchanges OHLC, stock data, etc.


## How to setup the project 

1. After cloning this repo, cd into the root directory and create a python virtual env
```
python3 -m venv venv
```

2. Once virtual env was setup, activate the virtual env. 
```
source venv/bin/activate
```

3. Next, you will need to install all the required packages for this. 
```
pip install -r requirements.txt
```

4. Next make sure to create `.env` file that contain all the credentials to your services like DB, API key etc. Here are the template for `.env` file.
```
POSTGRES_USER='your_username' 
POSTGRES_PWD='your_password' 
POSTGRES_PORT='5432'
POSTGRES_DBNAME='postgres'
POSTGRES_HOST='localhost'
```

5. We will be using dockerize posgresql database for storing the data. We will persist the data into the data folder home directory, so we will need to create this data folder folder. 
```
mkdir -p ~/data/postgres
```

6. Once all packages are installed and necessary folder are created, you should start your dockerize postgresdb. Please ensure the `.env` is created since we are using those creds in the docker compose. 
```
docker compose up -d 
```

7. Now, you can ran the first flow which was to create all the required tables
```
python run_flow.py create-table
```

8. Prefect come with the local server where you can monitor your flow activities in a pretty dashboard. You can start the server by using 
```
prefect server start
```



### Public Data Sources
- [Coinmetrics CSV](https://coinmetrics.io/community-network-data/)
- [Coingecko API](https://docs.coingecko.com/reference/introduction)
- [Coinmarketcap](https://coinmarketcap.com/currencies/bitcoin/historical-data/)
- [Open Exchange rate](https://docs.openexchangerates.org/reference/api-introduction)
- [SGD current rates](https://www.sgrates.com/bankrate/dbs.html)
- [Etherscan](https://docs.etherscan.io/) 
- [Chainz Explorer](https://chainz.cryptoid.info/api.dws)
- [Binance Exchange Public Data](https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md#klinecandlestick-data)
- [Kraken Public Data](https://docs.kraken.com/rest/#tag/Spot-Market-Data/operation/getOHLCData)
- [Crypto.com Public Data](https://exchange-docs.crypto.com/exchange/v1/rest-ws/index.html#reference-and-market-data-api)
- [Gemini Public Data](https://docs.gemini.com/rest-api/#symbols)
- [Alpha Vantage Stock API](https://www.alphavantage.co/documentation/)
