import requests 
from configs.settings import ETHERSCAN_APIKEY

class EtherscanApi():

    '''
    Simple API Wrapper to Etherscan REST API endpoints: 
    - token transactions (ERC-20)
    - normal transactions 
    - internal transactions

    '''

    def __init__(self):

        self.key = ETHERSCAN_APIKEY 
        self.base_url = 'https://api.etherscan.io/api'
        self.response = None 

        self.session = requests.Session()

        self._json_options = {}
    
    def _send_req(self, urlpath, params, headers=None, timeout=None):
        params = {} if params is None else params 
        headers = {} if headers is None else headers

        url = self.base_url + urlpath 

        self.response = self.session.get(url, params=params, headers=headers, timeout=timeout)

        return self.response.json(**self._json_options)
    
    def _get_erc20_txes(self, contract_addr=None, address=None, sort_order='asc', startb=0, endb=99999999):
        params = {
            'module': 'account',
            'action': 'tokentx',
            'page': '1',
            'offset': '10000',
            'startblock': startb,
            'endblock': endb,
            'sort': sort_order,
            'apikey': self.key
        }
        if contract_addr is not None: 
            params['contractaddress'] = contract_addr
        if address is not None: 
            params['address'] = address
        
        return self._send_req('', params=params)
        
