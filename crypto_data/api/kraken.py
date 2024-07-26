import requests

class API():

    '''
    Simple API Wrapper for KRAKEN REST API in their API Docs. 
    - Public Spot Market Data - DONE 
    - Public NFT Market Data - TODO
    - Private Endpoint - TODO 
    '''

    def __init__(self, key='', secret=''):
        
        version = '0.1.0'
        lib_url = "https://github.com/inotives"

        self.key = key 
        self.secret = secret 
        self.base_url = 'https://api.kraken.com'
        self.apiver = '0'
        self.response = None

        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'inotives/' + version + ' (+' + lib_url + ')',
            'Content-Type': 'application/x-www-form-urlencoded'
        })

        self._json_options = {} # params
        
        return 
    

    def _send_req (self, urlpath, data, headers=None, timeout=None):

        data = {} if data is None else data 
        headers = {} if headers is None else headers

        url = self.base_url + urlpath

        if 'public' in urlpath: 
            self.response = self.session.get(url, params=data, headers=headers, timeout=timeout)
        else: 
            self.response = self.session.post(url, data=data, headers=headers, timeout=timeout)

        if self.response.status_code not in (200, 201, 202):
            self.response.raise_for_status()
        
        return self.response.json(**self._json_options)
    
    def _get_public_data(self, endpoint, data=None, timeout=None ):
        data = {} if data is None else data 

        endpoint_path = f"/{self.apiver}/public/{endpoint}"

        return self._send_req(urlpath=endpoint_path, data=data, timeout=timeout)
    
    # Public Market Data - Get Assets Infos 
    def _get_pub_asset_infos(self):
        return self._get_public_data('Assets')
    
    def _get_pub_asset_pairs(self, params=None): 
        '''
        params: 
        - pair (optional): trading pair in comma list e.q. ETHXBT,XBTUSDC
        - infos (optional): Info to retrieve. Enum: info, leverage, fees, margin
        ''' 
        params = {} if params is None else params
        return self._get_public_data('AssetPairs', params)
    
    def _get_pub_ticker(self, params=None): 
        '''
        params: 
        - pair (optional): trading pair in comma list 
        '''
        params = {} if params is None else params
        return self._get_public_data('Ticker', params)
    
    def _get_pub_ohlc(self, params):
        '''
        params: 
        - pair (required): Asset pair to get data for. e.q. XBTUSDC
        - interval (optional): Time frame interval in minutes. Default: 1, Enum: 1 5 15 30 60 240 1440 10080 21600
        - since (optional): Return up to 720 OHLC data points since given timestamp. e.q since=1616663618
        '''
        return self._get_public_data('OHLC', params)
    
    def _get_pub_orderbook(self, params):
        '''
        params: 
        - pair (required): Asset pair to get data for. e.q. XBTUSDC
        - count (optional): Maximum number of asks/bids. Default: 100, Range: [1..500]
        '''
        return self._get_public_data('Depth', params)
    
    def _get_pub_recent_trades(self, params):
        '''
        params: 
        - pair (required): Asset pair to get data for. e.q. XBTUSDC
        - since (optional): Return trade data since given timestamp. e.q: since=1616663618
        - count (optional): Maximum number of asks/bids. Default: 1000, Range: [1..1000]
        '''
        return self._get_public_data('Trades', params)
    
    def _get_pub_recent_spreads(self, params):
        '''
        params: 
        - pair (required): Asset pair to get data for. e.q. XBTUSDC
        - since (optional): Returns spread data since given timestamp. intended for incremental updates within available dataset (does not contain all historical spreads).
        '''
        return self._get_public_data('Spread', params)
    

    # TODO -- PUBLIC NFT Market Data ENDPOINTS ------------------------------------------------------------------------------------------------

    # TODO -- PRIVATE ENDPOINTS ---------------------------------------------------------------------------------------------------------------
