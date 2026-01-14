import time

import pytest
from binance.websocket.websocket_client import BinanceWebsocketClient

from pycrypto.broker.websocket import BinanceWebsocket
from pycrypto.commons import Cache

Cache()


def test_websocket_must_create_correct_string_connections():
    ws = BinanceWebsocket()
    base_url_single = ws.base_url + "/ws/"
    base_url_stream = ws.base_url + "/stream?streams="

    assert ws.get_string_connection("BTCUSDT", "1m") == base_url_single + "btcusdt@kline_1m"
    assert (
        ws.get_string_connection("BTCUSDT", ["1s", "1h", "1d"])
        == base_url_stream + "btcusdt@kline_1s/btcusdt@kline_1h/btcusdt@kline_1d"
    )

    assert len(ws.subscribe_list) == 3
    assert "btcusdt@kline_1s" in ws.subscribe_list
    assert "btcusdt@kline_1h" in ws.subscribe_list
    assert "btcusdt@kline_1d" in ws.subscribe_list


@pytest.mark.binance_websocket
def test_websocket_start_must_append_data_on_cache():
    Cache.flushdb()
    params = {"ticker": "BTCUSDT", "intervals": ["1s", "1m", "1h"]}
    ws = BinanceWebsocket(**params)
    ws.start_websocket()
    time.sleep(5)
    assert isinstance(ws.stream, BinanceWebsocketClient)
    ws.close_websocket()

    assert len(Cache.get_klines("BTCUSDT", "1s")) >= 1  # 1s ever closed
    assert len(Cache.get_klines("BTCUSDT", "1m", closed_klines=False)) >= 1
    assert len(Cache.get_klines("BTCUSDT", "1h", closed_klines=False)) >= 1

    Cache.flushdb()
