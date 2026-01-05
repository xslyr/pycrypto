

from app.broker.binance_websocket_v1 import Binance_Websocket
from app.commons.utils import Singleton
from app.commons import Database, Cache
from .binance_spot_v1 import Binance_Spot_v1
from typing import Any
import logging


# TODO: incluir verificacao no obj spot

logger = logging.getLogger("app.spot")

class Broker (metaclass=Singleton):
  
    def __init__(self, test_mode=False):

        try: Database()
        except Exception as e: logger.warning(e)

        try: Cache()
        except Exception as e: logger.warning(e)

        self.test_mode = test_mode
        try: self.spot = Binance_Spot_v1(test_mode)
        except: logger.exception('Error on binance connection. Please verify environment variables or internet')
        self.websocket:Binance_Websocket
        self._trade_fee = None

    @property
    def wallet(self):
        return self.spot.wallet()

    @property
    def trade_fee(self):
        if self._trade_fee is None:
            self._trade_fee = self.spot.trade_fee
        return self._trade_fee()

    def start_websocket(self, ticker='BTCUSDT', intervals=['1s','1m','1h']):
        self.websocket = Binance_Websocket(ticker, intervals)
        self.websocket.start_websocket()

    def stop_websocket(self):
        try: 
            self.websocket.close_websocket()
            return True
        except: 
            return False

    def get_klines(self, ticker:str, interval:str, start_time:Any, as_dict=False, limit:int=1000):
        return self.spot.klines(ticker, interval, start_time, as_dict, limit)


    def buy(self, ticker:str, quantity:int, type='MARKET'):
        return self.spot.buy(ticker, quantity, type)


    def sell(self, ticker:str, quantity:int, type='MARKET'):
        return self.spot.sell(ticker, quantity, type)

