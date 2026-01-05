
import pytest, psycopg
import numpy as np
import pandas as pd

from app.commons import Database
from app.data_orchestration import Loader
from datetime import datetime, timedelta


def test_simple_connection():
    conn = Database().conn
    assert isinstance( conn, psycopg.Connection) == True


@pytest.mark.delete_db_data
def test_destroy_all_tables():
    assert Database().destroy_all_tables(confirmation=False) == False
    assert Database().destroy_all_tables(confirmation=True) == True


@pytest.mark.delete_db_data
def test_db_integrity():
    cb = Database()
    cb.destroy_all_tables(confirmation=True)
    assert cb.check_fix_db_integrity() == True


@pytest.mark.delete_db_data
def test_clean_kline_table():
    cb = Database()
    cb.check_fix_db_integrity()
    assert cb.clean_kline_table(['1d']) == True


@pytest.mark.delete_db_data
def test_insert_klines():
    cb = Database()
    cb.check_fix_db_integrity()
    cb.clean_kline_table(['1m'])
    f_opentime = lambda x: int( ( datetime(2025,1,1) + timedelta(minutes=x) ).timestamp()*1000 )
    arr1 = np.array( [ f_opentime(i) for i in range(5) ]   ).reshape(-1,1)
    arr2 = np.array( [list(range(11))]*5 )
    data = pd.DataFrame(np.hstack((arr1, arr2))).to_records(index=False).tolist()
    assert cb.insert_klines('BTCUSDT','1m',data) == True



# Testes para o Loader_Data, onde há o insert de dados fictícios

@pytest.mark.delete_db_data
def test_select_klines():
    cb = Database()
    cb.check_fix_db_integrity()
    cb.clean_kline_table(['1m'])
    f_opentime = lambda x: int( ( datetime(2025,1,1) + timedelta(minutes=x) ).timestamp()*1000 )
    arr1 = np.array( [f_opentime(i) for i in range(5)] ).reshape(-1,1)
    arr2 = np.array( [list(range(11))]*5 )
    data = pd.DataFrame(np.hstack((arr1, arr2))).to_records(index=False).tolist()
    cb.insert_klines('BTCUSDT','1m',data)
    assert len( cb.select_klines('BTCUSDT','1m', from_datetime=datetime(2025,1,1)) ) == 5


@pytest.mark.delete_db_data
def test_dump_data():
    cbd = Database()    
    cbd.check_fix_db_integrity()
    cbd.clean_kline_table(['1d'])

    d = datetime(2025,1,1)
    qty_intervals = int((datetime.now()-d )/timedelta(days=1))
    Loader().dump_klines_into_db('BTCUSDT',['1d'], from_datetime=d, verbose=True)
    qty_records = len(cbd.select_klines('BTCUSDT','1d', from_datetime=d))
    assert qty_records == qty_intervals
    
    Loader().dump_klines_into_db('BTCUSDT',['1d'], from_datetime=d)
    assert qty_records == qty_intervals