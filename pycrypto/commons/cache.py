import json
import logging
import os
from datetime import datetime
from operator import itemgetter
from typing import Any

import redis
from redis.exceptions import ConnectionError as RedisConnectionError, ResponseError
from redis.typing import ResponseT

from pycrypto.commons.utils import BrokerUtils, Singleton

logger = logging.getLogger("app")


class Cache(metaclass=Singleton):
    _cache: redis.Redis = None  # type: ignore
    key_types = {}

    def __init__(self):
        if Cache._cache is None:
            params = {
                "host": os.getenv("REDIS_HOST", "localhost"),
                "db": 0,
                "port": int(os.getenv("REDIS_PORT", "6379")),
                "socket_keepalive": True,
                "health_check_interval": 30,
                "decode_responses": True,
            }
            Cache._maxlen = BrokerUtils.websocket_opened_maxlen
            try:
                Cache._cache = redis.Redis(**params)
                Cache._cache.ping()
            except RedisConnectionError:
                logger.exception(
                    "Redis connection error. Please verify environment variables and service availability."
                )
                raise

    @classmethod
    def save(cls, key: str, value: Any) -> bool:
        try:
            match value:
                case int():
                    str_value = str(value)
                    cls.key_types[key] = lambda x: int(x)
                case float():
                    str_value = str(value)
                    cls.key_types[key] = lambda x: float(x)
                case dict() | list():
                    str_value = json.dumps(value)
                    cls.key_types[key] = lambda x: json.loads(x)
                case datetime():
                    str_value = str(value)
                    cls.key_types[key] = lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S")
                case str():
                    str_value = value
                    cls.key_types[key] = lambda x: x

            cls._cache.set(key, str_value)
            return True

        except Exception:
            logger.exception(f"Error on save key {key}, value {value} on redis.")
            raise

    @classmethod
    def get(cls, key: str):
        try:
            return cls.key_types[key](cls._cache.get(key))
        except Exception:
            logger.warning(f"Key {key} not exists.")
            return None

    @classmethod
    def search_keys(cls, pattern="*"):
        return cls._cache.keys(pattern)

    @classmethod
    def append_kline(cls, stream: Any, data: dict, closed_klines=True) -> bool | None:
        match stream:
            case str():
                redis_key = stream
            case tuple() | list():
                redis_key = "@kline_".join(stream)
            case _:
                raise Exception(
                    "Stream parameter must be a tuple of (ticker,interval) or string like ticker@kline_interval."
                )

        redis_key = redis_key.lower()
        redis_key += ":closed" if closed_klines else ":opened"
        maxlen = cls._maxlen[stream[1]] if isinstance(stream, list | tuple) else cls._maxlen[stream.split("_")[1]]

        try:
            opentime = data.get("open_time") or data.get("t")
            cls._cache.xadd(
                redis_key,
                {"data": json.dumps(data)},
                id=f"{opentime}-*",
                maxlen=maxlen,
                approximate=True,
            )
            return True
        except Exception:
            logger.exception(f"Error on append_klines {stream}")
            return None

    @classmethod
    def append_klines(cls, stream: Any, data: list[dict], closed_klines=True) -> bool | None:
        pipe = cls._cache.pipeline()
        match stream:
            case str():
                redis_key = stream
            case tuple() | list():
                redis_key = "@kline_".join(stream)
            case _:
                raise Exception(
                    "Stream parameter must be a tuple of (ticker,interval) or string like ticker@kline_interval."
                )

        redis_key = redis_key.lower()
        redis_key += ":closed" if closed_klines else ":opened"
        maxlen = cls._maxlen[stream[1]] if isinstance(stream, list | tuple) else cls._maxlen[stream.split("_")[1]]

        try:
            for item in data:
                pipe.xadd(
                    redis_key,
                    {"data": json.dumps(item)},
                    id=item["open_time"],
                    maxlen=maxlen,
                    approximate=True,
                )
            pipe.execute()
            return True
        except Exception:
            logger.exception(f"Error on append_klines {stream}")
            return None

    @classmethod
    def get_klines(
        cls,
        ticker: str,
        interval: str,
        min_range="-",
        max_range="+",
        closed_klines=True,
        limit=None,
        only_columns=[],
    ) -> list | None:
        def get_data(x):
            return json.loads(x[1]["data"])

        try:
            redis_key = f"{ticker.lower()}@kline_{interval}"
            redis_key += ":closed" if closed_klines or interval == "1s" else ":opened"
            _limit = cls._maxlen[interval] if not limit else limit

            raw_data = cls._cache.xrevrange(redis_key, max=max_range, min=min_range, count=_limit)

            r = (
                [get_data(d) for d in raw_data]
                if only_columns == []
                else [itemgetter(*only_columns, get_data(d)) for d in raw_data]
            )

            return r

        except Exception:
            logger.warning(f"Error on get_klines {redis_key}. Check if this key exists.")
            return None

    @classmethod
    def get_info_stream(cls, ticker: str, interval: str, closed_klines=True) -> dict | ResponseT:
        try:
            redis_key = f"{ticker.lower()}@kline_{interval}"
            redis_key += ":closed" if closed_klines else ":opened"
            return cls._cache.xinfo_stream(redis_key)

        except ResponseError:
            logger.warning(f"The key {redis_key} not exists.")
            return {}

        except Exception:
            logger.exception(f"Error on get_info_stream of ticker {ticker}, interval {interval}")
            raise

    @classmethod
    def delete_stream(cls, ticker: str, interval: str, closed_klines=True) -> bool:
        _stream = f"{ticker.lower()}@kline_{interval}"
        _stream += ":closed" if closed_klines else ":opened"
        try:
            cls._cache.delete(_stream)
            return True
        except Exception:
            logger.warning(f"Error on delete stream of ticker {ticker}, interval {interval}")
            return False

    @classmethod
    def check_key_exists(cls, ticker: str, interval: str, closed_klines=True) -> bool:
        redis_key = f"{ticker.lower()}@kline_{interval}"
        redis_key += ":closed" if closed_klines else ":opened"
        return bool(cls._cache.exists(redis_key))

    @classmethod
    def delete_ids_in_stream(cls, stream: Any, list_ids: list, closed_klines=True) -> bool:
        match stream:
            case str():
                _stream = stream
            case tuple() | list():
                _stream = "@kline_".join(stream)
            case _:
                raise Exception(
                    "Stream parameter must be a tuple of (ticker,interval) or string like ticker@kline_interval."
                )

        _stream = _stream.lower()
        _stream += ":closed" if closed_klines else ":opened"

        pipe = cls._cache.pipeline()
        try:
            pipe.xdel(_stream, list_ids)
            return True
        except Exception:
            logger.exception("Error on deletion stream data.")
            raise

    @classmethod
    def delete_data_in_stream(cls, ticker: str, interval: str, nlast: int = 0, closed_klines=True):
        pipe = cls._cache.pipeline()

        _stream = f"{ticker.lower()}@kline_{interval}"
        _stream += ":closed" if closed_klines else ":opened"
        try:
            pipe.xtrim(_stream, maxlen=nlast, approximate=True)
            return True
        except Exception:
            logger.warning(
                f"Error on deletion stream data. Ticker: {ticker}, Interval: {interval}, closed_klines: {closed_klines}."
            )
            return None

    @classmethod
    def flushdb(cls) -> bool:
        try:
            cls._cache.flushdb()
            return True
        except Exception:
            logger.warning("Error on flush redis.")
            raise
