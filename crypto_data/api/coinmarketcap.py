from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json
import configs.settings as st


def test_conn():
    url = 'https://sandbox-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
    parameters = {
    'start': '1',
    'limit': '5000',
    'convert': 'USD'
    }
    headers = {
    'Accepts': 'application/json',
    'X-CMC_PRO_API_KEY': st.CMC_APIKEY,
    }

    session = Session()
    session.headers.update(headers)

    try:
        response = session.get(url, params=parameters)
        data = json.loads(response.text)
        print(data)  # Python 3 print function
    except (ConnectionError, Timeout, TooManyRedirects) as e:
        print(e)  # Python 3 print function
