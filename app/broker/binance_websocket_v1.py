
from typing import overload
from app.commons.utils import Singleton
from app.commons import Cache
from binance.websocket.websocket_client import BinanceWebsocketClient
import logging, json


# https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Kline-Candlestick-Streams

logger = logging.getLogger("app.websocket")

class Binance_Websocket(metaclass=Singleton):

    _stream: BinanceWebsocketClient = None # type:ignore

    def __init__(self, ticker='BTCUSDT', intervals=['1s','1m','1h'], **kwargs):
        Cache()
        self.base_url = 'wss://stream.binance.com:9443'
        self.ticker = ticker
        self.intervals = intervals


    @property
    def stream(self):
        return self._stream
    

    def get_string_connection(self, ticker:str=None, intervals:str|list=None):
        
        if not ticker: ticker = self.ticker
        if not intervals: intervals = self.intervals
        subscribe_list = []
        append_str=''
        if isinstance(intervals, str): 
            append_str += f'/ws/'
            stream_name=f'{ticker.lower()}@kline_{intervals}/'
            subscribe_list.append( stream_name[:-1] )
            append_str += stream_name
        else:
            if len(intervals) == 1:
                append_str += f'/ws/'
                stream_name = f'{ticker.lower()}@kline_{intervals[0]}/'
                subscribe_list.append( stream_name[:-1] )
                append_str += stream_name
            else:
                append_str+='/stream?streams='
                for item in intervals:
                    stream_name = f'{ticker.lower()}@kline_{item}/'
                    subscribe_list.append( stream_name[:-1] )
                    append_str += stream_name
        
        self.subscribe_list = subscribe_list
        string_connection = str(self.base_url + append_str)[:-1]
        logger.debug(f'String connection: {string_connection}')
        return string_connection


    def on_multi_klines_message(self, ws, message):
        try:
            logger.debug(message)
            msg = json.loads(message)
            Cache.append_kline(msg['stream'], msg['data']['k'], closed_klines=msg['data']['k']['x'])
        except: pass


    def on_single_message(self, ws, message):
        try:
            logger.debug(message)
            msg = json.loads(message)
            stream_name = msg['s'], msg['k']['i']
            Cache.append_kline(stream_name, msg['k'], closed_klines=msg['k']['x'])
        except: pass


    def pong(self, ws, message):
        self._stream.subscribe( self.subscribe_list, id=message.decode('utf-8'))


    def on_error(self, ws, message):
        logger.error(message)
        #pass


    def close_websocket(self):
        self._stream.stop()


    @overload
    def start_websocket(self, ticker:str, intervals:str|list):...
    @overload
    def start_websocket(self, string_connection:str):...
    
    def start_websocket(self, *args, **kwargs):
        try:    
            match len(args):
                case 1: string_connection = args[0]
                case 2: 
                    ticker, intervals = args[0], args[1] 
                    string_connection = self.get_string_connection(ticker,intervals)
                case _:
                    string_connection = kwargs.get('string_connection','')
                    ticker = kwargs.get('ticker','')
                    intervals = kwargs.get('intervals',[])
            
            if not string_connection and not all([ticker, intervals]):
                ticker = self.ticker
                intervals = self.intervals
                string_connection = self.get_string_connection( ticker, intervals)
                
            logger.info(f'Starting websocket for ticker {ticker} with intervals { ', '.join(intervals) }.')
            multi_klines_stream = len(intervals) > 1
            
            if multi_klines_stream:
                self._stream = BinanceWebsocketClient(
                    stream_url=string_connection,
                    on_message=self.on_multi_klines_message,
                    on_ping=self.pong,
                    on_error=self.on_error
                )
            else:
                self._stream = BinanceWebsocketClient(
                    stream_url=string_connection,
                    on_message=self.on_single_message,
                    on_ping=self.pong,
                    on_error=self.on_error
                )
            return True
        except:
            logger.exception(f'Error on starting websocket.')