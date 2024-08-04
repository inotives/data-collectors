import requests 

class API():

    def __init__(self, key='', secret=''):
        
        self.key = key
        self.secret = secret 
        self.base_url = 'https://community-api.coinmetrics.io'
        self.apiver = 'v4'
        self.response = None 

        self.session = requests.Session()
        self._json_options = {} # for params 

        return 
    
    def _send_req(self, urlpath, data, headers=None, timeout=None):

        data = {} if data is None else data 
        headers = {} if headers is None else headers

        url = f"{self.base_url}/{self.apiver}/{urlpath}"

        self.response = self.session.get(url, params=data, headers=headers, timeout=timeout)

        return self.response.json(**self._json_options)
    
    def _get_coinmetric_daily(self, asset_list, start_date, end_date, page_size=10000, sortby='time'):
        metrics = 'AdrActCnt,CapAct1yrUSD,CapMVRVCur,CapMVRVFF,CapMrktCurUSD,CapMrktEstUSD,CapMrktFFUSD,CapRealUSD,FeeMeanNtv,FeeMeanUSD,FeeMedNtv,FeeMedUSD,FeeTotNtv,FeeTotUSD,PriceBTC,PriceUSD,SplyFF,SplyCur,TxCnt,TxTfrCnt,TxTfrValAdjNtv,TxTfrValAdjUSD,TxTfrValMeanNtv,TxTfrValMeanUSD,TxTfrValMedNtv,TxTfrValMedUSD,SER,AdrBalUSD100Cnt,AdrBalUSD1KCnt,AdrBalUSD10KCnt,AdrBalUSD100KCnt,AdrBalUSD1MCnt,SplyAdrBalUSD100,SplyAdrBalUSD1K,SplyAdrBalUSD10K,SplyAdrBalUSD100K,SplyAdrBalUSD1M,SplyAct1d,SplyAct30d,SplyAct1yr,SplyAct90d,VtyDayRet30d'
        params = {
            'assets': asset_list,
            'metrics' : metrics,
            'frequency': '1d',
            'start_time': start_date,
            'end_time': end_date,
            'page_size': page_size,
            'sort': sortby
        }

        return self._send_req(urlpath='/timeseries/asset-metrics', data=params )