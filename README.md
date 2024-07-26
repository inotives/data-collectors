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

4. Once all packages are installed, you should start your dockerize postgresdb. 
```
docker compose up -d 
```

5. Now, you can ran the first flow which was to create all the required tables
```
python run_flow.py
```

6. Prefect come with the local server where you can monitor your flow activities in a pretty dashboard. You can start the server by using 
```
prefect server start
```

