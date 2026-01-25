import numpy as np
import pytest

from pycrypto.commons.database import Database
from pycrypto.commons.utils import DataSources
from pycrypto.trading import ItemRule, convert_data_to_numpy
from pycrypto.trading.technical_analysis import Overlap
from tests.broker_wrapper import BrokerWrapper

raw_data = BrokerWrapper().get_klines("BTCUSDT", "1h", "2025-01-01 00:00:00")
data = convert_data_to_numpy(raw_data, DataSources.mock)


def test_convert_mock_data_to_numpy():
    assert not isinstance(raw_data, np.ndarray)
    data = convert_data_to_numpy(raw_data, DataSources.mock)
    assert isinstance(data, np.ndarray)


def test_convert_db_data_to_numpy():
    db_data = Database().select_klines("BTCUSD", "1d")
    arr_db = convert_data_to_numpy(db_data, DataSources.database)
    assert isinstance(arr_db, np.ndarray)


@pytest.mark.parametrize(
    "key, operator, value, expected",
    [
        ("open", ">", 96165.29, False),
        ("open", ">=", 96165.29, True),
        ("open", "<", 96165.29, False),
        ("open", "<=", 96165.29, True),
        ("open", "!=", 96165.29, False),
        ("open", "==", 96165.29, True),
        ("close", ">", 95469.28, False),
        ("close", ">=", 95469.28, True),
        ("close", "<", 95469.28, False),
        ("close", "<=", 95469.28, True),
        ("close", "!=", 95469.28, False),
        ("close", "==", 95469.28, True),
        (None, ">", 95469.28, False),
        (None, ">=", 95469.28, True),
        (None, "<", 95469.28, False),
        (None, "<=", 95469.28, True),
        (None, "!=", 95469.28, False),
        (None, "==", 95469.28, True),
    ],
)
def test_itemrule_must_be_comparable_with_a_number(key, operator, value, expected):
    arr = ItemRule(data, key)  # noqa: F841
    assert eval(f"arr {operator} {value}") == expected


def test_itemrule_must_be_comparable_one_each_other():
    arr_close = ItemRule(data, "close")
    arr_open = ItemRule(data, "open")
    # open=96165.29  close=95469.28
    assert not arr_close > arr_open
    assert not arr_close >= arr_open
    assert arr_close < arr_open
    assert arr_close <= arr_open
    assert not arr_close == arr_open
    assert arr_close != arr_open


def test_itemrule_binded_must_compared_one_each_other_by_function_result():
    arr_sma = ItemRule(data, Overlap.sma)
    sma_result = Overlap.sma(data)
    assert arr_sma == sma_result[-1].item()
    arr_bolinger = ItemRule(data, Overlap.bollinger, target_operator=1)
    bollinger_result = Overlap.bollinger(data)
    assert arr_bolinger == bollinger_result[1][-1].item()


def test_itemrule_must_return_active_field():
    arr_close = ItemRule(data, "close")
    assert arr_close._active_field == "close"
    assert isinstance(arr_close, ItemRule)
    assert all(np.asarray(ItemRule(data).active_field) == data["close"])


def test_itemrule_must_return_last_line():
    arr_close = ItemRule(data, "close")
    assert isinstance(arr_close.last, np.ndarray)
    assert isinstance(arr_close.last[0], np.void)
    assert np.asarray(arr_close.last) == data[-1:]
    assert arr_close.last[0] == data[-1]


def test_itemrule_must_return_equal_result_on_run_fnc_of_talib_func():
    arr_close = ItemRule(data, "close", Overlap.sma, {"length": 5})
    arr_run = arr_close.run()
    sma_result = Overlap.sma(data, length=5, target="close")
    np.testing.assert_array_equal(arr_run, sma_result)


def test_itemrule_must_allow_init_void():
    arr_void = ItemRule()
    assert isinstance(arr_void, ItemRule)
    assert arr_void._active_field == "close"
    assert arr_void.active_field.size == 0
    assert isinstance(arr_void.active_field, np.ndarray)
    assert isinstance(arr_void.last, np.ndarray)


def test_itemrule_must_bind_array_after_init():
    arr = ItemRule()
    assert all([arr.active_field.size == 0, arr._active_field == "close", arr._params == {}])
    arr.bind("open")
    assert arr._active_field == "open"
    arr.bind({"length": 10})
    assert arr._params == {"length": 10}
    arr.bind(data)
    assert arr.active_field.size > 0
