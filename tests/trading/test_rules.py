import numpy as np
import pytest

from pycrypto.trading import ItemRule
from pycrypto.trading.technical_analysis import Overlap


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
def test_itemrule_must_be_comparable_with_a_number(numpy_data, key, operator, value, expected):
    arr = ItemRule(numpy_data, key)  # noqa: F841
    assert eval(f"arr {operator} {value}") == expected


def test_itemrule_must_be_comparable_one_each_other(numpy_data):
    arr_close = ItemRule(numpy_data, "close")
    arr_open = ItemRule(numpy_data, "open")
    # open=96165.29  close=95469.28
    assert not arr_close > arr_open
    assert not arr_close >= arr_open
    assert arr_close < arr_open
    assert arr_close <= arr_open
    assert not arr_close == arr_open
    assert arr_close != arr_open


def test_itemrule_binded_must_compared_one_each_other_by_function_result(numpy_data):
    arr_sma = ItemRule(numpy_data, Overlap.sma)
    sma_result = Overlap.sma(numpy_data)
    assert arr_sma == sma_result[-1].item()
    arr_bolinger = ItemRule(numpy_data, Overlap.bollinger, target_operator=1)
    bollinger_result = Overlap.bollinger(numpy_data)
    assert arr_bolinger == bollinger_result[1][-1].item()


def test_itemrule_must_return_active_field(numpy_data):
    arr_close = ItemRule(numpy_data, "close")
    assert arr_close._active_field == "close"
    assert isinstance(arr_close, ItemRule)
    assert all(np.asarray(ItemRule(numpy_data).active_field) == numpy_data["close"])


def test_itemrule_must_return_last_line(numpy_data):
    arr_close = ItemRule(numpy_data, "close")
    assert isinstance(arr_close.last, np.ndarray)
    assert isinstance(arr_close.last[0], np.void)
    assert np.asarray(arr_close.last) == numpy_data[-1:]
    assert arr_close.last[0] == numpy_data[-1]


def test_itemrule_must_return_equal_result_on_run_fnc_of_talib_func(numpy_data):
    arr_close = ItemRule(numpy_data, "close", Overlap.sma, {"length": 5})
    arr_run = arr_close.run()
    sma_result = Overlap.sma(numpy_data, length=5, target="close")
    np.testing.assert_array_equal(arr_run, sma_result)


def test_itemrule_must_allow_init_void():
    arr_void = ItemRule()
    assert isinstance(arr_void, ItemRule)
    assert arr_void._active_field == "close"
    assert arr_void.active_field.size == 0
    assert isinstance(arr_void.active_field, np.ndarray)
    assert isinstance(arr_void.last, np.ndarray)


def test_itemrule_must_bind_array_after_init(numpy_data):
    arr = ItemRule()
    assert all([arr.active_field.size == 0, arr._active_field == "close", arr._params == {}])
    arr.bind("open")
    assert arr._active_field == "open"
    arr.bind({"length": 10})
    assert arr._params == {"length": 10}
    arr.bind(numpy_data)
    assert arr.active_field.size > 0
