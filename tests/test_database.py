from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import psycopg
import pytest

from pycrypto.commons import Database
from pycrypto.orchestration import Loader


def f_opentime(x):
    return int((datetime(2025, 1, 1) + timedelta(minutes=x)).timestamp() * 1000)


def test_dbclass_must_connect():
    conn = Database().conn
    assert isinstance(conn, psycopg.Connection)


@pytest.mark.delete_db_data
def test_dbclass_can_delete_all_tables():
    assert not Database().destroy_all_tables(confirmation=False)
    assert Database().destroy_all_tables(confirmation=True)


@pytest.mark.delete_db_data
def test_dbclass_must_check_integrity():
    cb = Database()
    cb.destroy_all_tables(confirmation=True)
    assert cb.check_fix_db_integrity()


@pytest.mark.delete_db_data
def test_dbclass__can_clean_kline_tables():
    cb = Database()
    cb.check_fix_db_integrity()
    assert cb.clean_kline_table(["1d"])


@pytest.mark.delete_db_data
def test_dbclass_can_insert_klines():
    cb = Database()
    cb.check_fix_db_integrity()
    cb.clean_kline_table(["1m"])
    arr1 = np.array([f_opentime(i) for i in range(5)]).reshape(-1, 1)
    arr2 = np.array([list(range(11))] * 5)
    data = pd.DataFrame(np.hstack((arr1, arr2))).to_records(index=False).tolist()
    assert cb.insert_klines("BTCUSDT", "1m", data)


# Testes para o Loader_Data, onde há o insert de dados fictícios


@pytest.mark.delete_db_data
def test_dbclass_can_select_klines():
    cb = Database()
    cb.check_fix_db_integrity()
    cb.clean_kline_table(["1m"])
    arr1 = np.array([f_opentime(i) for i in range(5)]).reshape(-1, 1)
    arr2 = np.array([list(range(11))] * 5)
    data = pd.DataFrame(np.hstack((arr1, arr2))).to_records(index=False).tolist()
    cb.insert_klines("BTCUSDT", "1m", data)
    assert len(cb.select_klines("BTCUSDT", "1m", from_datetime=datetime(2025, 1, 1))) == 5


@pytest.mark.delete_db_data
def test_broker_loader_must_dump_data_in_kline_tables():
    cbd = Database()
    cbd.check_fix_db_integrity()
    cbd.clean_kline_table(["1d"])

    d = datetime(2025, 1, 1)
    qty_intervals = int((datetime.now() - d) / timedelta(days=1))
    Loader().dump_klines_into_db("BTCUSDT", ["1d"], from_datetime=d, verbose=True)
    qty_records = len(cbd.select_klines("BTCUSDT", "1d", from_datetime=d))
    assert qty_records == qty_intervals

    Loader().dump_klines_into_db("BTCUSDT", ["1d"], from_datetime=d)
    assert qty_records == qty_intervals
