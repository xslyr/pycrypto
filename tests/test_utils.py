

from app.commons.utils import Timing as Tm
from datetime import datetime


def test_convert_any_to_datetime():
    assert Tm.convert_any_to_datetime('1986-01-30 06:00:00') == datetime(1986,1,30,6,0,0)
    assert Tm.convert_any_to_datetime(507456000.0) == datetime(1986,1,30,6,0,0)
    assert Tm.convert_any_to_datetime(507456000) == datetime(1986,1,30,6,0,0)
    assert Tm.convert_any_to_datetime(507456000000) == datetime(1986,1,30,6,0,0)


def test_convert_any_to_timestamp():
    assert Tm.convert_any_to_timestamp( '2025-01-01 00:00:00') == 1735700400000
    assert Tm.convert_any_to_timestamp( datetime(2025,1,1,0,0,0) ) == 1735700400000
    assert Tm.convert_any_to_timestamp( 1735700400.0 ) == 1735700400000
    assert Tm.convert_any_to_timestamp( 1735700400 ) == 1735700400000