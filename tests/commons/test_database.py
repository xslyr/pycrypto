from typing import List

import numpy as np
import pytest
from sqlalchemy.orm import Session

from pycrypto import db
from pycrypto.commons.models_main import Klines_1d
from pycrypto.commons.utils import BrokerUtils, convert_data_to_numpy


def test_dbclass_must_connect():
    # here we check the real session used by application
    # on another cases, we will use main_db on memory via fixture db_temp
    with db.session_factory() as session:
        assert isinstance(session, Session)


@pytest.mark.delete_db_data
def test_dbclass_can_clean_kline_tables():
    data = [
        {
            "open_time": 1735776000000,
            "open": "94591.78000000",
            "high": "97839.50000000",
            "low": "94392.00000000",
            "close": "96984.79000000",
            "base_asset_volume": "21970.48948000",
            "close_time": 1735862399999,
            "quote_asset_volume": "2118411852.68127950",
            "number_of_trades": 3569079,
            "taker_buy_base_asset_volume": "10915.96986000",
            "taker_buy_quote_asset_volume": "1052226710.33367170",
        }
    ]
    params = {"ticker": "BTCUSDT", "interval": "1d"}
    db.insert_klines(data=data, **params)
    result = db.select_klines(**params)
    assert len(result) >= 1
    db.clean_kline_table([params["interval"]])
    result = db.select_klines(**params)
    assert len(result) == 0


@pytest.mark.delete_db_data
def test_dbclass_can_insert_klines_as_tuple(broker):
    data = broker.get_klines("BTCUSDT", "1d", "2025-01-01 00:00:00")
    db.clean_kline_table(["1d"])
    assert db.insert_klines("BTCUSDT", "1d", data)


@pytest.mark.delete_db_data
def test_dbclass_can_insert_klines_as_dict(broker):
    base_from = "2025-01-01 00:00:00"
    db.clean_kline_table(["1m"])
    data_dict = broker.get_klines("BTCUSDT", "1d", start_time=base_from, as_dict=True)
    assert db.insert_klines("BTCUSDT", "1d", data_dict)


# ---
@pytest.mark.delete_db_data
def test_dbclass_can_select_klines(broker):
    base_from = "2025-01-01 00:00:00"
    data = broker.get_klines("BTCUSDT", "1d", base_from)
    data_len = len(data)
    db.clean_kline_table(["1d"])
    db.insert_klines("BTCUSDT", "1d", data)
    assert len(db.select_klines("BTCUSDT", "1d", from_datetime=base_from)) == data_len


@pytest.mark.parametrize(
    "params, text_to_assertion",
    [
        ({"ticker": "BTCUSDT", "interval": "X"}, "Interval not available."),
        (
            {"ticker": "BTCUSDT", "interval": "1d", "returns": "dict", "cols": ["open", "close", "x"]},
            "Some column are not available.",
        ),
        (
            {"ticker": "BTCUSDT", "interval": "1d", "returns": "tuple", "cols": ["open", "close", "x"]},
            "Some column are not available.",
        ),
    ],
)
def test_dbclass_must_return_exception_with_unreconized_interval(cleaned_1d_table_scenario, params, text_to_assertion):
    with pytest.raises(Exception) as error:
        db.select_klines(**params)
        assert text_to_assertion in str(error.value)


@pytest.mark.parametrize(
    "params, result",
    [
        ({"ticker": "BTCUSDT", "interval": "1d"}, Klines_1d),
        ({"ticker": "BTCUSDT", "interval": "1d", "from_datetime": "2025-01-01 00:00:00"}, Klines_1d),
        (
            {
                "ticker": "BTCUSDT",
                "interval": "1d",
                "between_datetimes": ("2025-01-01 00:00:00", "2025-01-10 00:00:00"),
            },
            Klines_1d,
        ),
        ({"ticker": "BTCUSDT", "interval": "1d", "returns": "dict"}, dict),
        ({"ticker": "BTCUSDT", "interval": "1d", "returns": "dict", "cols": ["open", "close"]}, dict),
        ({"ticker": "BTCUSDT", "interval": "1d", "returns": "tuple"}, tuple),
        ({"ticker": "BTCUSDT", "interval": "1d", "returns": "tuple", "cols": ["open", "close"]}, tuple),
    ],
)
def test_dbclass_can_select_with_all_ways_mode(cleaned_1d_table_scenario, params, result):
    select = db.select_klines(**params)
    assert len(select) > 1
    assert isinstance(select[0], result)
    if params.get("cols") and params.get("dict"):
        assert len(dict(select[0]).keys()) == 2

    if params.get("cols") and params.get("tuple"):
        assert len(select[0]) == 2


@pytest.mark.delete_db_data
def test_dbclass_can_select_klines_without_datetime_params(cleaned_1d_table_scenario):
    cols_pattern = BrokerUtils.kline_columns[2:-1]
    data = cleaned_1d_table_scenario

    klines = db.select_klines("BTCUSDT", "1d", returns="dict", cols=cols_pattern)
    data_arr = convert_data_to_numpy(data, cols=cols_pattern)
    select_arr = convert_data_to_numpy(klines, cols=cols_pattern)

    assert isinstance(klines, List)
    np.testing.assert_array_equal(select_arr, data_arr)
