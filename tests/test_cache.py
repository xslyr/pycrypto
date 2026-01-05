
from dotenv import load_dotenv
from app.commons.cache import Cache
from app.commons.utils import BrokerUtils
from app.broker import Broker
from icecream import ic
from datetime import datetime, timedelta, time
import os

Cache()
load_dotenv()
test_mode = True

def get_klines(ticker='BTCUSDT', interval='1h', delta=500):
    today = datetime.combine( datetime.now().date(), time(0,0))
    from_datetime = today - timedelta(hours=delta)
    return Broker(test_mode).get_klines(ticker, interval, from_datetime, as_dict=True)   

BTCUSDT_1H_500 = get_klines()


def test_save_get_and_flush():
    Cache.flushdb()
    assert Cache.save('key-str','value-str') == True
    assert Cache.save('key-int', 123 ) == True
    assert Cache.save('key-float', 1.23 ) == True
    assert Cache.save('key-dict', {'value': 123}) == True
    assert Cache.save('key-list', [1,2,3] ) == True
    assert Cache.save('key-datetime', datetime(2025,12,18,8,55,00) ) == True

    assert Cache.get('key-str') == 'value-str'
    assert Cache.get('key-int') == 123
    assert Cache.get('key-float') == 1.23
    assert Cache.get('key-dict') == {'value': 123}
    assert Cache.get('key-list') == [1,2,3]
    assert Cache.get('key-datetime') == datetime(2025,12,18,8,55,00) 

    Cache.flushdb()
    assert Cache.search_keys() == []

def test_append_kline_and_klines():
    Cache.flushdb()
    ticker, interval = 'BTCUSDT','1h'
    assert Cache.append_klines( (ticker,interval), BTCUSDT_1H_500[:-1] ) == True
    assert Cache.append_kline( (ticker,interval), BTCUSDT_1H_500[-1] ) == True
        

def test_get_kline():
    Cache.flushdb()
    ticker, interval = 'BTCUSDT','1h'
    assert Cache.append_kline((ticker,interval), BTCUSDT_1H_500[0])
    klines = Cache.get_klines(ticker, interval)
    assert isinstance( klines, list )
    assert isinstance( klines[0], dict)
    Cache.flushdb()


def test_delete_data_in_stream():
    Cache.flushdb()
    ticker, interval = 'BTCUSDT','1h'
    cache_len = int(os.getenv('REDIS_KLINES_MAXLEN',100))
    assert Cache.append_klines((ticker, interval), BTCUSDT_1H_500)
    assert Cache.delete_data_in_stream('BTCUSDT','1h', nlast=10) == True
    Cache.flushdb()


def test_check_key_exists():
    Cache.flushdb()
    ticker, interval = 'BTCUSDT','1h'
    assert Cache.append_kline( (ticker,interval), BTCUSDT_1H_500[0])
    assert Cache.check_key_exists('BTCUSDT','1h') == True
    Cache.flushdb()


def test_get_info_stream():
    Cache.flushdb()
    ticker, interval = 'BTCUSDT','1h'
    cache_len = int(os.getenv('REDIS_KLINES_LEN',100))
    assert Cache.append_klines((ticker,interval), BTCUSDT_1H_500)
    assert isinstance( Cache.get_info_stream('BTCUSDT','1h'), dict)
    


def test_delete_stream():
    Cache.flushdb()
    ticker, interval = 'BTCUSDT','1h'
    assert Cache.append_klines((ticker,interval), BTCUSDT_1H_500)
    assert Cache.delete_stream('BTCUSDT','1h') == True
    assert Cache.check_key_exists('BTCUSDT','1h') == False
    Cache.flushdb()


def test_klines_maxlen():
    Cache.flushdb()
    ticker, interval = 'BTCUSDT', '1h'
    env_maxlen = BrokerUtils.websocket_opened_maxlen[interval]
    assert Cache.append_klines( (ticker,interval), BTCUSDT_1H_500 )
    ic('The max of 10% aproximation is considering for redis obtain maximum performance on klines IO operations.')
    assert 0.85*env_maxlen <= len( Cache.get_klines(ticker, interval) ) <= 1.15*env_maxlen

    Cache.flushdb()