from datetime import datetime, time, timedelta

import pytest
from dotenv import load_dotenv

from pycrypto.broker import Broker
from pycrypto.commons.cache import Cache
from pycrypto.commons.utils import BrokerUtils

Cache()
load_dotenv()
test_mode = True


def get_klines(ticker="BTCUSDT", interval="1h", delta=500):
    today = datetime.combine(datetime.now().date(), time(0, 0))
    from_datetime = today - timedelta(hours=delta)
    return Broker(test_mode).get_klines(ticker, interval, from_datetime, as_dict=True)


BTCUSDT_1H_500 = get_klines()


def test_cache_must_delete_all_keys():
    Cache.flushdb()
    assert Cache.search_keys() == []


@pytest.mark.parametrize(
    "key, value",
    [
        ("key-str", "value-str"),
        ("key-int", 123),
        ("key-float", 1.23),
        ("key-dict", {"value": 123}),
        ("key-list", [1, 2, 3]),
        ("key-datetime", datetime(2025, 12, 18, 8, 55, 00)),
    ],
)
def test_cache_must_save_get_and_flush_simple_data(key, value):
    Cache.flushdb()
    assert Cache.save(key, value)
    assert Cache.get(key) == value


@pytest.mark.binance_request
def test_cache_must_append_a_simple_kline_and_mutiples_klines():
    Cache.flushdb()
    ticker, interval = "BTCUSDT", "1h"
    assert Cache.append_klines((ticker, interval), BTCUSDT_1H_500[:-1])
    assert Cache.append_kline((ticker, interval), BTCUSDT_1H_500[-1])


@pytest.mark.binance_request
def test_cache_must_get_klines():
    Cache.flushdb()
    ticker, interval = "BTCUSDT", "1h"
    assert Cache.append_kline((ticker, interval), BTCUSDT_1H_500[0])
    klines = Cache.get_klines(ticker, interval)
    assert isinstance(klines, list)
    assert isinstance(klines[0], dict)
    Cache.flushdb()


@pytest.mark.binance_request
def test_cache_must_delete_data_in_stream():
    Cache.flushdb()
    ticker, interval = "BTCUSDT", "1h"
    assert Cache.append_klines((ticker, interval), BTCUSDT_1H_500)
    assert Cache.delete_data_in_stream("BTCUSDT", "1h", nlast=10)
    Cache.flushdb()


@pytest.mark.binance_request
def test_cache_must_check_if_key_exists():
    Cache.flushdb()
    ticker, interval = "BTCUSDT", "1h"
    assert Cache.append_kline((ticker, interval), BTCUSDT_1H_500[0])
    assert Cache.check_key_exists("BTCUSDT", "1h")
    Cache.flushdb()


@pytest.mark.binance_request
def test_cache_must_get_info_about_stream():
    Cache.flushdb()
    ticker, interval = "BTCUSDT", "1h"
    # cache_len = int(os.environ["REDIS_KLINES_LEN", 100))
    assert Cache.append_klines((ticker, interval), BTCUSDT_1H_500)
    assert isinstance(Cache.get_info_stream("BTCUSDT", "1h"), dict)


@pytest.mark.binance_request
def test_cache_can_delete_stream():
    Cache.flushdb()
    ticker, interval = "BTCUSDT", "1h"
    assert Cache.append_klines((ticker, interval), BTCUSDT_1H_500)
    assert Cache.delete_stream("BTCUSDT", "1h")
    assert not Cache.check_key_exists("BTCUSDT", "1h")
    Cache.flushdb()


@pytest.mark.binance_request
def test_cache_appendklines_must_satisfy_maxlen_criteria():
    Cache.flushdb()
    ticker, interval = "BTCUSDT", "1h"
    env_maxlen = BrokerUtils.websocket_opened_maxlen[interval]
    assert Cache.append_klines((ticker, interval), BTCUSDT_1H_500)
    assert 0.85 * env_maxlen <= len(Cache.get_klines(ticker, interval)) <= 1.15 * env_maxlen

    Cache.flushdb()
