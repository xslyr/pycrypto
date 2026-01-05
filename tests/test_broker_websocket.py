from app.commons import Cache
from app.broker.binance_websocket_v1 import Binance_Websocket
from binance.websocket.websocket_client import BinanceWebsocketClient
import pytest, time
from icecream import ic

Cache()

@pytest.mark.broker
def test_get_string_connection():
    ws = Binance_Websocket()
    base_url_single = ws.base_url+'/ws/'
    base_url_stream = ws.base_url+'/stream?streams='
    
    ic('Checking string connection creation.')
    assert ws.get_string_connection('BTCUSDT','1m') == base_url_single + 'btcusdt@kline_1m'
    assert ws.get_string_connection('BTCUSDT',['1s','1h','1d']) == base_url_stream+'btcusdt@kline_1s/btcusdt@kline_1h/btcusdt@kline_1d'
    
    ic('Checking if subscribe list is ok to perform correct ping-pong reply.')
    assert len(ws.subscribe_list) == 3
    assert 'btcusdt@kline_1s' in ws.subscribe_list
    assert 'btcusdt@kline_1h' in ws.subscribe_list
    assert 'btcusdt@kline_1d' in ws.subscribe_list


@pytest.mark.broker
def test_start_websocket():
    Cache.flushdb()
    params = { 'ticker': 'BTCUSDT', 'intervals': ['1s','1m','1h'] }
    ws = Binance_Websocket(**params)
    ws.start_websocket()
    time.sleep(5)
    assert isinstance( ws.stream, BinanceWebsocketClient )
    ws.close_websocket()
   
    ic('Checking if redis contains stream keys ...')
    assert len( Cache.get_klines('BTCUSDT','1s') ) >= 1 # 1s ever closed
    assert len( Cache.get_klines('BTCUSDT','1m', closed_klines=False) ) >= 1
    assert len( Cache.get_klines('BTCUSDT','1h', closed_klines=False) ) >= 1

    Cache.flushdb()
    
    