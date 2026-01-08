from datetime import datetime

import pytest

from pycrypto.commons.cache import Cache
from pycrypto.commons.utils import BrokerUtils
from tests.broker_wrapper import BrokerWrapper

Cache()


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


def test_cache_must_append_a_simple_kline_and_mutiples_klines():
    Cache.flushdb()
    ticker, interval, start_time = "BTCUSDT", "1h", datetime(2025, 1, 1, 0, 0, 0)
    data = BrokerWrapper().get_klines(ticker, interval, start_time)
    assert Cache.append_klines((ticker, interval), data)


def test_cache_must_get_klines():
    Cache.flushdb()
    ticker, interval, start_time = "BTCUSDT", "1h", datetime(2025, 1, 1, 0, 0, 0)
    data = BrokerWrapper().get_klines(ticker, interval, start_time)
    assert Cache.append_klines((ticker, interval), data)
    klines = Cache.get_klines(ticker, interval)
    assert isinstance(klines, list)
    assert isinstance(klines[0], dict)
    Cache.flushdb()


def test_cache_must_delete_data_in_stream():
    Cache.flushdb()
    ticker, interval, start_time = "BTCUSDT", "1h", datetime(2025, 1, 1, 0, 0, 0)
    data = BrokerWrapper().get_klines(ticker, interval, start_time)
    assert Cache.append_klines((ticker, interval), data)
    assert Cache.delete_data_in_stream("BTCUSDT", "1h", nlast=10)
    Cache.flushdb()


def test_cache_must_check_if_key_exists():
    Cache.flushdb()
    ticker, interval, start_time = "BTCUSDT", "1h", datetime(2025, 1, 1, 0, 0, 0)
    data = BrokerWrapper().get_klines(ticker, interval, start_time)
    assert Cache.append_kline((ticker, interval), data[0])  # kline insert 1 item, klines multiple items
    assert Cache.check_key_exists(ticker, interval)
    Cache.flushdb()


def test_cache_must_get_info_about_stream():
    Cache.flushdb()
    ticker, interval, start_time = "BTCUSDT", "1h", datetime(2025, 1, 1, 0, 0, 0)
    data = BrokerWrapper().get_klines(ticker, interval, start_time)
    assert Cache.append_klines((ticker, interval), data)
    assert isinstance(Cache.get_info_stream("BTCUSDT", "1h"), dict)


def test_cache_can_delete_stream():
    Cache.flushdb()
    ticker, interval, start_time = "BTCUSDT", "1h", datetime(2025, 1, 1, 0, 0, 0)
    data = BrokerWrapper().get_klines(ticker, interval, start_time)
    assert Cache.append_klines((ticker, interval), data)
    assert Cache.delete_stream("BTCUSDT", "1h")
    assert not Cache.check_key_exists("BTCUSDT", "1h")
    Cache.flushdb()


def test_cache_appendklines_must_satisfy_maxlen_criteria():
    Cache.flushdb()
    ticker, interval, start_time = "BTCUSDT", "1h", datetime(2025, 1, 1, 0, 0, 0)
    data = BrokerWrapper().get_klines(ticker, interval, start_time)
    env_maxlen = BrokerUtils.websocket_opened_maxlen[interval]
    assert Cache.append_klines((ticker, interval), data)
    assert 0.85 * env_maxlen <= len(Cache.get_klines(ticker, interval)) <= 1.15 * env_maxlen

    Cache.flushdb()
