from operator import itemgetter

import numpy as np
import pytest

from pycrypto.broker.utils import aggregate_structured, convert_spotklines_to_numpy, ticker_contains, upsert_monitor_arr

# Mock necessário para a função convert_spotklines_to_numpy
columns_dtype = {"ticker": ("ticker", "U10"), "open_time": ("open_time", "i8"), "price": ("price", "f8")}


def test_upsert_monitor_arr():
    dt = [("ticker", "U10"), ("timestamp", "i8"), ("price", "f8")]

    storage = np.array([("BTCUSDT", 100, 50000.0), ("ETHUSDT", 50, 2000.0)], dtype=dt)

    new_batch = np.array(
        [
            ("BTCUSDT", 110, 50100.0),  # Update: timestamp maior
            ("LTCUSDT", 30, 100.0),  # Novo ticker
        ],
        dtype=dt,
    )

    result = upsert_monitor_arr(storage, new_batch)

    # Deve conter 3 tickers únicos
    assert result.size == 3
    # BTCUSDT deve ter o timestamp 110 (o mais recente)
    btc_idx = np.where(result["ticker"] == "BTCUSDT")[0][0]
    assert result[btc_idx]["timestamp"] == 110
    assert result[btc_idx]["price"] == 50100.0
    # ETHUSDT deve permanecer
    assert "ETHUSDT" in result["ticker"]


def test_upsert_monitor_arr_empty_storage():
    dt = [("ticker", "U10"), ("timestamp", "i8")]
    storage = np.empty(0, dtype=dt)
    new_batch = np.array([("BTCUSDT", 100)], dtype=dt)

    result = upsert_monitor_arr(storage, new_batch)
    assert result.size == 1
    assert result[0]["ticker"] == "BTCUSDT"


def test_aggregate_structured():
    dt = [("ticker", "U10"), ("val", "f8"), ("qty", "i8")]
    data = np.array([("A", 10.0, 1), ("A", 20.0, 2), ("B", 5.0, 10)], dtype=dt)

    agg_dict = {"val": np.sum, "qty": np.max}
    result = aggregate_structured(data, "ticker", agg_dict)

    assert result.size == 2
    row_a = result[result["ticker"] == "A"][0]
    assert row_a["val"] == 30.0
    assert row_a["qty"] == 2


def test_ticker_contains():
    dt = [("ticker", "U10"), ("id", "i4")]
    data = np.array([("BTCUSDT", 1), ("ETHBTC", 2), ("SOLUSDC", 3)], dtype=dt)

    res_usdt = ticker_contains(data, "USDT")
    assert res_usdt.size == 1
    assert res_usdt[0]["ticker"] == "BTCUSDT"

    res_btc = ticker_contains(data, "BTC")
    assert res_btc.size == 2

    res_none = ticker_contains(data, "DOGE")
    assert res_none.size == 0


def test_convert_spotklines_to_numpy(broker):
    params = {"ticker": "BTCUSDT", "interval": "1m", "limit": 1000}
    dict_data = broker.get_klines(*params.values(), as_dict=True)
    tuple_data = [tuple(row.values()) for row in dict_data]

    cols = list(dict_data[0].keys())
    result = convert_spotklines_to_numpy(tuple_data, cols, ignore_last_column=False)

    assert isinstance(result, np.ndarray)
    assert result.dtype.names == tuple(cols)
    assert result.size == len(tuple_data)
    assert result["close"][0] == np.float64(dict_data[0]["close"])
