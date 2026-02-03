from datetime import datetime
from zoneinfo import ZoneInfo

import numpy as np
import pytest

from pycrypto import db
from pycrypto.commons.utils import convert_any_to_datetime, convert_any_to_timestamp


@pytest.mark.parametrize(
    "param", ["1986-01-30 08:00:00", 507456000.0, 507456000, 507456000000, datetime(1986, 1, 30, 8, 0, 0)]
)
def test_convert_any_to_datetime_must_works(param):
    result = convert_any_to_datetime(param)
    assert result == datetime(1986, 1, 30, 8, 0, 0, tzinfo=ZoneInfo("UTC"))


@pytest.mark.parametrize(
    "param",
    ["2025-01-01 03:00:00", datetime(2025, 1, 1, 3, 0, 0), 1735700400.0, 1735700400],
)
def test_convert_any_to_timestamp_must_works(param):
    result = convert_any_to_timestamp(param)

    expected = datetime.fromtimestamp(1735700400, tz=ZoneInfo("UTC"))

    assert result == expected.timestamp() * 1000
