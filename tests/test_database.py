from datetime import datetime, timedelta

import numpy as np
import psycopg
import pytest

from pycrypto.commons import Database
from pycrypto.orchestration import Loader
from tests.broker_wrapper import BrokerWrapper

br = BrokerWrapper(test_mode=True)
base_from = datetime(2025, 1, 1)
data = br.get_klines("BTCUSDT", "1d", base_from)
data_len = len(data)


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
def test_dbclass_can_insert_klines_as_tuple():
    cb = Database()
    cb.check_fix_db_integrity()
    cb.clean_kline_table(["1d"])
    assert cb.insert_klines("BTCUSDT", "1d", data)


@pytest.mark.delete_db_data
def test_dbclass_can_insert_klines_as_dict():
    cb = Database()
    cb.check_fix_db_integrity()
    cb.clean_kline_table(["1m"])
    data_dict = br.get_klines("BTCUSDT", "1d", start_time=base_from, as_dict=True)
    assert cb.insert_klines("BTCUSDT", "1d", data_dict)


@pytest.mark.delete_db_data
def test_dbclass_can_select_klines():
    cb = Database()
    cb.check_fix_db_integrity()
    cb.clean_kline_table(["1d"])
    cb.insert_klines("BTCUSDT", "1d", data)
    assert len(cb.select_klines("BTCUSDT", "1d", from_datetime=base_from)) == data_len
