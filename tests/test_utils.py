from datetime import datetime, timedelta, timezone

import pytest

from pycrypto.commons.utils import Timing as Tm

tz = timezone(timedelta(hours=0))


@pytest.mark.parametrize("param", ["1986-01-30 06:00:00", 507456000.0, 507456000, 507456000000])
def test_convert_any_to_datetime_must_works(param):
    assert Tm.convert_any_to_datetime(param) == datetime(1986, 1, 30, 6, 0, 0).astimezone(tz)


@pytest.mark.parametrize(
    "param",
    ["2025-01-01 00:00:00", datetime(2025, 1, 1, 0, 0, 0).astimezone(tz), 1735700400.0, 1735700400],
)
def test_convert_any_to_timestamp_must_works(param):
    assert Tm.convert_any_to_timestamp(param) == 1735700400000
