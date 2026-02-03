import numpy as np

from pycrypto import db
from pycrypto.orchestration.utils import convert_data_to_numpy


def test_convert_dict_data_to_numpy(broker):
    raw_data = broker.get_klines("BTCUSDT", "1h", "2025-01-01 00:00:00")
    assert not isinstance(raw_data, np.ndarray)
    data = convert_data_to_numpy(raw_data)
    assert isinstance(data, np.ndarray)


def test_convert_tuple_data_to_numpy():
    db_data = db.select_klines("BTCUSD", "1d", returns="tuple")
    arr_db = convert_data_to_numpy(db_data)
    assert isinstance(arr_db, np.ndarray)
