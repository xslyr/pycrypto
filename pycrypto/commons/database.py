import logging
import os
from contextlib import contextmanager
from typing import Any, Dict

import numpy as np

# import psycopg
# from psycopg import OperationalError
from sqlalchemy import create_engine, delete, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

import pycrypto.commons.models_main as m

from .utils import Singleton

logger = logging.getLogger("app")


# TODO: Will be better change to SQLAlchemy?
class Database(metaclass=Singleton):
    ModelMapping = {
        "1s": m.Klines_1s,
        "1m": m.Klines_1m,
        "3m": m.Klines_3m,
        "5m": m.Klines_5m,
        "15m": m.Klines_15m,
        "30m": m.Klines_30m,
        "1h": m.Klines_1h,
        "2h": m.Klines_2h,
        "4h": m.Klines_4h,
        "6h": m.Klines_6h,
        "8h": m.Klines_8h,
        "12h": m.Klines_12h,
        "1d": m.Klines_1d,
    }

    def __init__(self, **kwargs):
        host, port, dbname, user, password = tuple(
            map(os.getenv, ["POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD"])
        )

        self.__connection_str = f"postgresql+psycopg://{user}:{password}@{host}:{port}/{dbname}"
        self.__engine = kwargs.get(
            "mock_engine",
            create_engine(self.__connection_str, pool_size=5, max_overflow=10, pool_timeout=20),
        )

    @property
    def connection_str(self):
        return self.__connection_str

    @contextmanager
    def session_factory(self):
        with Session(self.__engine) as session:
            yield session

    def clean_kline_table(self, intervals: list[str]):
        models = [self.ModelMapping.get(i) for i in intervals]

        if len(models) == 0:
            logger.error(f"Models not found to intervals {intervals}.")
            raise

        try:
            with self.session_factory() as session:
                for m in models:
                    stmt = delete(m)
                    session.execute(stmt)
                session.commit()
        except Exception:
            logger.exception("Error on clen_kline_table.")
            raise

    def select_klines(
        self,
        ticker: str,
        interval: str,
        from_datetime: Any = "",
        between_datetimes: tuple[Any, Any] = ("", ""),
        **kwargs,
    ):
        return_as = kwargs.get("return_as", "object")

        model_class = self.ModelMapping.get(interval)

        if not model_class:
            logger.error(f"Models not found to interval {interval}.")
            raise

        if from_datetime == "" and between_datetimes == ("", ""):
            stmt = select(model_class).where(model_class.ticker == ticker)

        if from_datetime != "":
            stmt = select(model_class).where(model_class.ticker == ticker, model_class.open_time >= from_datetime)

        if all(between_datetimes):
            stmt = select(model_class).where()

        with self.session_factory() as session:
            result = session.execute(stmt)
            if return_as == "tuple":
                return result

            return result.scalars().all()

    def insert_klines(self, ticker: str, interval: str, data: np.ndarray | list[Dict]) -> bool:
        model_class = self.ModelMapping.get(interval)

        if not model_class:
            logger.error(f"Models not found to interval {interval}.")
            return False

        try:
            if isinstance(data, np.ndarray):
                columns = ("ticker",) + data.dtype.names
                data_to_insert = [dict(zip(columns, (ticker,) + row)) for row in data.tolist()]
            else:
                data_to_insert = [dict({"ticker": ticker}).update(row) for row in data]

            stmt = insert(model_class).values(data_to_insert)
            stmt = stmt.on_conflict_do_nothing(index_elements=["ticker", "open_time"])

            with self.session_factory() as session:
                session.execute(stmt)
                session.commit()

            return True

        except Exception as e:
            logger.exception(f"Error klines insertion ({ticker}, {interval}): {e}")
            return False
