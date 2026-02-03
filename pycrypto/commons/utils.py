import time
from datetime import datetime, timedelta
from enum import Enum
from typing import Any
from zoneinfo import ZoneInfo

# https://python-binance.readthedocs.io/en/latest/constants.html

klines_intervals_available = ["1s", "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d"]
KlinesIntervals = Enum("KlinesIntervals", klines_intervals_available)

widemonitor_intervals_available = ["1h", "4h", "1d"]
WidemonitorIntervals = Enum("WidemonitorIntervals", widemonitor_intervals_available)

delta_intervals = {
    "1s": timedelta(seconds=1),
    "1m": timedelta(minutes=1),
    "3m": timedelta(minutes=3),
    "5m": timedelta(minutes=5),
    "15m": timedelta(minutes=15),
    "30m": timedelta(minutes=30),
    "1h": timedelta(hours=1),
    "2h": timedelta(hours=2),
    "4h": timedelta(hours=4),
    "6h": timedelta(hours=6),
    "8h": timedelta(hours=8),
    "12h": timedelta(hours=12),
    "1d": timedelta(days=1),
}

default_tz = ZoneInfo("UTC")


def convert_any_to_datetime(_datetime: Any):
    """Method to convert any datatype for datetime"""
    match _datetime:
        case str():
            if len(_datetime) != 19:
                raise Exception(
                    "On datetime param we expect str with 19 chars. e.g. 2023-01-01 00:00:00 \n You also consider send timestamp or datetime obj param."
                )
            adjusted_start_time = datetime.strptime(_datetime, "%Y-%m-%d %H:%M:%S")
        case int() | float():
            if len(str(int(_datetime))) > 10:
                adjusted_start_time = datetime.fromtimestamp(_datetime / 1000, tz=default_tz)
            else:
                adjusted_start_time = datetime.fromtimestamp(_datetime, tz=default_tz)
        case datetime():
            adjusted_start_time = _datetime

    return adjusted_start_time.replace(tzinfo=default_tz)


def convert_any_to_timestamp(_datetime: Any):
    """Method for convert any datatype for timestamp"""
    match _datetime:
        case str():
            if len(_datetime) != 19:
                raise Exception(
                    "On datetime param we expect str with 19 chars. e.g. 2023-01-01 00:00:00 \n You also consider send timestamp or datetime obj param."
                )
            dt = datetime.strptime(_datetime, "%Y-%m-%d %H:%M:%S")
            dt = dt if time.tzname[0] == "UTC" else dt.astimezone(default_tz)
            adjusted_start_time = int(dt.timestamp() * 1000)

        case int() | float():
            if len(str(_datetime)) < 13:
                adjusted_start_time = int(str(int(_datetime)).ljust(13, "0"))
            else:
                adjusted_start_time = _datetime

        case datetime():
            dt = _datetime if time.tzname[0] == "UTC" else _datetime.replace(tzinfo=ZoneInfo("UTC"))
            adjusted_start_time = int(dt.timestamp() * 1000)

        case _:
            raise Exception("Unknown timestamp format.")

    return adjusted_start_time


def get_timestamp_range_list(start: datetime, end: datetime, interval: str):
    """Method to generate a timestamp range list between a datetime intervals"""

    _start = int(start.timestamp())
    _end = int(end.timestamp())
    _steps = int(delta_intervals[interval].total_seconds())

    return list(range(_start, _end + 1, _steps))


class Singleton(type):
    """Metaclass who implements parent structure of singleton objects"""

    _instance = {}

    def __call__(cls, **kwargs):
        if cls not in cls._instance:
            cls._instance[cls] = super().__call__(**kwargs)
        return cls._instance[cls]

    @classmethod
    def _reset_all(mcs):
        mcs._instance.clear()
