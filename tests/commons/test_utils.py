from datetime import datetime

import numpy as np
import pytest

from pycrypto import db
from pycrypto.commons.utils import convert_any_to_datetime, convert_any_to_timestamp, convert_data_to_numpy


@pytest.mark.parametrize(
    "param", ["1986-01-30 06:00:00", 507456000.0, 507456000, 507456000000, datetime(1986, 1, 30, 6, 0, 0)]
)
def test_convert_any_to_datetime_must_works(param):
    assert convert_any_to_datetime(param) == datetime(1986, 1, 30, 6, 0, 0)


@pytest.mark.parametrize(
    "param",
    ["2025-01-01 00:00:00", datetime(2025, 1, 1, 0, 0, 0), 1735700400.0, 1735700400],
)
def test_convert_any_to_timestamp_must_works(param):
    assert convert_any_to_timestamp(param) == 1735700400000


def test_convert_dict_data_to_numpy(broker):
    raw_data = broker.get_klines("BTCUSDT", "1h", "2025-01-01 00:00:00")
    assert not isinstance(raw_data, np.ndarray)
    data = convert_data_to_numpy(raw_data)
    assert isinstance(data, np.ndarray)


def test_convert_tuple_data_to_numpy():
    db_data = db.select_klines("BTCUSD", "1d", returns="tuple")
    arr_db = convert_data_to_numpy(db_data)
    assert isinstance(arr_db, np.ndarray)
