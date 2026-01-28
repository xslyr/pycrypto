import os
from datetime import datetime, timedelta
from enum import Enum
from operator import itemgetter
from typing import Any, Dict
from zoneinfo import ZoneInfo

import numpy as np

# https://python-binance.readthedocs.io/en/latest/constants.html

DataSources = Enum("OriginsAvailable", ["database", "websocket", "mock"])


class Timing:
    """Class responsable for aggregate timing context constants and common functions correlated with it."""

    klines_intervals_available = [
        "1s",
        "1m",
        "3m",
        "5m",
        "15m",
        "30m",
        "1h",
        "2h",
        "4h",
        "6h",
        "8h",
        "12h",
        "1d",
    ]
    widemonitor_intervals_available = ["1h", "4h", "1d"]

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

    tz = ZoneInfo(os.getenv("TZ", "UTC"))

    @staticmethod
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
                    adjusted_start_time = datetime.fromtimestamp(_datetime / 1000, tz=Timing.tz)
                else:
                    adjusted_start_time = datetime.fromtimestamp(_datetime, tz=Timing.tz)
            case datetime():
                adjusted_start_time = _datetime

        return adjusted_start_time.replace(tzinfo=None)

    @staticmethod
    def convert_any_to_timestamp(_datetime: Any):
        """Method for convert any datatype for timestamp"""
        match _datetime:
            case str():
                if len(_datetime) != 19:
                    raise Exception(
                        "On datetime param we expect str with 19 chars. e.g. 2023-01-01 00:00:00 \n You also consider send timestamp or datetime obj param."
                    )
                adjusted_start_time = datetime.strptime(_datetime, "%Y-%m-%d %H:%M:%S").replace(tzinfo=Timing.tz)
                adjusted_start_time = int(adjusted_start_time.timestamp() * 1000)

            case int() | float():
                if len(str(_datetime)) < 13:
                    adjusted_start_time = int(str(int(_datetime)).ljust(13, "0"))
                else:
                    adjusted_start_time = _datetime

            case datetime():
                dt = _datetime.replace(tzinfo=Timing.tz) if _datetime.tzinfo is None else _datetime
                adjusted_start_time = int(dt.timestamp() * 1000)

            case _:
                raise Exception("Unknown timestamp format.")

        return adjusted_start_time

    @staticmethod
    def get_timestamp_range_list(start: datetime, end: datetime, interval: str):
        """Method to generate a timestamp range list between a datetime intervals"""

        _start = int(start.timestamp())
        _end = int(end.timestamp())
        _steps = int(Timing.delta_intervals[interval].total_seconds())

        return list(range(_start, _end + 1, _steps))


class DatabaseDescription:
    """Class responsable for aggregate database context constants and common functions correlated with it."""

    db_structure = {
        "app_config": {
            "columns": {"key": "varchar(50) not null", "value": "text"},
            "primary_key": "primary key (key)",
        },
        "klines_tables": {
            "columns": {
                "ticker": "varchar(10) not null",
                "open_time": "bigint not null",
                "open": "real not null",
                "high": "real not null",
                "low": "real not null",
                "close": "real not null",
                "base_asset_volume": "double precision not null",
                "close_time": "bigint not null",
                "quote_asset_volume": "double precision not null",
                "number_of_trades": "bigint not null",
                "taker_buy_base_asset_volume": "double precision not null",
                "taker_buy_quote_asset_volume": "double precision not null",
            },
            "primary_key": "primary key (ticker, open_time)",
        },
    }
    qty_tables = len(db_structure.keys()) + len(Timing.klines_intervals_available) - 1


class Singleton(type):
    """Metaclass who implements parent structure of singleton objects"""

    _instance = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instance:
            cls._instance[cls] = super().__call__(*args, **kwargs)
        return cls._instance[cls]

    @classmethod
    def _reset_all(mcs):
        mcs._instance.clear()


class BrokerUtils:
    """Class responsable for aggregate broker context constants and common functions correlated with it."""

    kline_columns = [
        "open_time",
        "close_time",
        "open",
        "close",
        "high",
        "low",
        "base_asset_volume",
        "quote_asset_volume",
        "number_of_trades",
        "taker_buy_base_asset_volume",
        "taker_buy_quote_asset_volume",
        "ignore",
    ]

    ws_columns_names = {
        "open_time": "t",
        "close_time": "T",
        "ticker": "s",
        "interval": "i",
        "first_trade_id": "f",
        "last_trade_id": "L",
        "open": "o",
        "close": "c",
        "high": "h",
        "low": "l",
        "base_asset_volume": "v",
        "number_of_trades": "n",
        "is_kline_closed": "x",
        "quote_asset_volume": "q",
        "taker_buy_base_asset_volume": "V",
        "taker_buy_quote_asset_volume": "Q",
        "ignore": "B",
    }

    columns_dtype = {
        "open_time": ("open_time", "i8"),
        "close_time": ("close_time", "i8"),
        "ticker": ("ticker", "S10"),
        "interval": ("interval", "S3"),
        "first_trade_id": ("first_trade_id", "i8"),
        "last_trade_id": ("last_trade_id", "i8"),
        "open": ("open", "f8"),
        "close": ("close", "f8"),
        "high": ("high", "f8"),
        "low": ("low", "f8"),
        "base_asset_volume": ("base_asset_volume", "f8"),
        "number_of_trades": ("number_of_trades", "i8"),
        "is_kline_closed": ("is_kline_closed", "?"),
        "quote_asset_volume": ("quote_asset_volume", "f8"),
        "taker_buy_base_asset_volume": ("taker_buy_base_asset_volume", "f8"),
        "taker_buy_quote_asset_volume": ("taker_buy_quote_asset_volume", "f8"),
        "ignore": ("ignore", "?"),
    }

    websocket_opened_maxlen = {
        "1s": 60,
        "1m": 60,
        "3m": 20,
        "5m": 12,
        "15m": 4,
        "30m": 2,
        "1h": 24,
        "2h": 12,
        "4h": 6,
        "6h": 4,
        "8h": 3,
        "12h": 2,
        "1d": 1,
    }

    widemonitor_columns = {
        "e": "etype",  # Event type (tipo de evento).
        "E": "timestamp",  # Event time (tempo do evento) em milissegundos desde a Época Unix.
        "s": "ticker",  # Símbolo do par de negociação.
        "p": "var_price",  # Variação de preço (price change) no período.
        "P": "pct_price",  # Variação percentual do preço (price change percentage) no período.
        "w": "vwap",  # Preço médio ponderado no período.
        "x": "last_price",  # Preço do último trade realizado antes do fechamento do ticker.
        "c": "close",  # Preço de fechamento no período.
        "Q": "last_qty",  # Quantidade do último trade realizado.
        "b": "best_bid_price",  # O melhor preço de compra no topo do livro (Bid).
        "B": "best_bid_qty",  # A quantidade disponível no melhor preço de compra.
        "a": "best_ask_price",  # O melhor preço de venda no topo do livro (Ask).
        "A": "best_ask_qty",  # A quantidade disponível no melhor preço de venda.
        "o": "open",  # Preço de abertura no período.
        "h": "high",  # Preço mais alto no período.
        "l": "low",  # Preço mais baixo no período.
        "v": "base_asset_volume",  # Volume total negociado no período.
        "q": "quote_asset_volume",  # Volume total em dólares negociado no período.
        "O": "open_time",  # Timestamp da abertura no período apresentado.
        "C": "close_time",  # Timestamp de fechamento no período apresentado.
        "F": "first_trade",  # Primeiro trade (primeiro negócio) no período.
        "L": "last_trade",  # Último trade (último negócio) no período.
        "n": "number_of_trades",  # Número total de trades no período.
    }

    widemonitor_columns_dtype = {
        "etype": ("etype", "U8"),
        "timestamp": ("timestamp", "i8"),
        "ticker": ("ticker", "U15"),
        "pct_price": ("pct_price", "f8"),
        "var_price": ("var_price", "f8"),
        "vwap": ("vwap", "f8"),
        "last_price": ("last_price", "f8"),
        "close": ("close", "f8"),
        "last_qty": ("last_qty", "f8"),
        "best_bid_price": ("best_bid_price", "f8"),
        "best_bid_qty": ("best_bid_qty", "f8"),
        "best_ask_price": ("best_ask_price", "f8"),
        "best_ask_qty": ("best_ask_qty", "f8"),
        "open": ("open", "f8"),
        "high": ("high", "f8"),
        "low": ("low", "f8"),
        "base_asset_volume": ("base_asset_volume", "f8"),
        "quote_asset_volume": ("quote_asset_volume", "f8"),
        "open_time": ("open_time", "f8"),
        "close_time": ("close_time", "f8"),
        "first_trade": ("first_trade", "i8"),
        "last_trade": ("last_trade", "i8"),
        "number_of_trades": ("number_of_trades", "i8"),
    }


def convert_data_to_numpy(data: list[Dict], origin: DataSources, **kwargs) -> np.array:
    """Method to prepare and convert crude data to numpy array"""
    result = None
    match origin:
        case DataSources.websocket:
            cols = kwargs.get("cols", BrokerUtils.kline_columns[2:-1])
            dtypes = list(itemgetter(*cols)(BrokerUtils.columns_dtype))
            col_ids = itemgetter(*cols)(BrokerUtils.ws_columns_names)
            arr = (itemgetter(*col_ids)(i) for i in data)
            result = np.fromiter(arr, dtype=dtypes)

        case DataSources.database:
            cols = kwargs.get("cols", BrokerUtils.kline_columns[2:-1])
            dtypes = list(itemgetter(*cols)(BrokerUtils.columns_dtype))
            arr = data
            result = np.array(arr, dtype=dtypes)

        case DataSources.mock:
            cols = kwargs.get("cols", data[0].keys())
            dtypes = list(itemgetter(*cols)(BrokerUtils.columns_dtype))
            arr = [tuple(itemgetter(*cols)(row)) for row in data]
            result = np.array(arr, dtype=dtypes)

    return result
