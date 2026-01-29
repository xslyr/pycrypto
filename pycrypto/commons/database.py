import logging
import os
from contextlib import contextmanager
from typing import Any, Dict

import numpy as np
from sqlalchemy import create_engine, delete, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

import pycrypto.commons.models_main as m
from pycrypto.commons.utils import Singleton, Timing, convert_any_to_timestamp

logger = logging.getLogger("app")


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
        self.__connection_str = kwargs.get("connection_str", "")
        configs = kwargs.get("configs", {})

        if not self.__connection_str:
            host, port, dbname, user, password = tuple(
                map(os.getenv, ["POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD"])
            )
            self.__connection_str = f"postgresql+psycopg://{user}:{password}@{host}:{port}/{dbname}"
            configs = {"pool_size": 5, "max_overflow": 10, "pool_timeout": 20}

        self.__engine = create_engine(self.__connection_str, **configs)

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
            err = f"Models not found to intervals {intervals}."
            logger.error(err)
            raise Exception(err)

        try:
            with self.session_factory() as session:
                for model in models:
                    session.execute(delete(model))
                session.commit()
        except Exception as e:
            logger.exception("Error on clen_kline_table.")
            raise e

    def select_klines(
        self,
        ticker: str,
        interval: str,
        from_datetime: Any = "",
        between_datetimes: tuple[Any, Any] = ("", ""),
        **kwargs,
    ):
        if interval not in Timing.klines_intervals_available:
            e = "Interval not available."
            logger.exception(e)
            raise Exception(e)

        returns = kwargs.get("returns", "model")
        cols = kwargs.get("cols", "")
        model = self.ModelMapping.get(interval)
        model_cols = None

        if returns in ["tuple", "dict"]:
            if cols == "":
                model_cols = model.__table__.columns
            else:
                try:
                    model_cols = [model.__table__.columns[c] for c in cols]
                except Exception:
                    e = "Some column are not available."
                    logger.exception(e)
                    raise

        match model_cols:
            case None:
                stmt = select(model)
            case list():
                stmt = select(*model_cols)
            case _:
                stmt = select(model_cols)

        if from_datetime == "" and between_datetimes == ("", ""):
            stmt = stmt.where(model.ticker == ticker)

        if from_datetime != "":
            start = convert_any_to_timestamp(from_datetime)
            stmt = stmt.where(model.ticker == ticker, model.open_time >= start)

        if all(between_datetimes):
            start = convert_any_to_timestamp(between_datetimes[0])
            end = convert_any_to_timestamp(between_datetimes[1])
            stmt = stmt.where(model.ticker == ticker, model.open_time.between(start, end + 1))

        with self.session_factory() as session:
            result = session.execute(stmt)
            match returns:
                case "model":
                    return result.scalars().all()
                case "dict":
                    return [row._asdict() for row in result]

            return [tuple(row) for row in result]

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
                data_to_insert = [{"ticker": ticker, **row} for row in data]

            stmt = insert(model_class).values(data_to_insert)
            stmt = stmt.on_conflict_do_nothing(index_elements=["ticker", "open_time"])

            with self.session_factory() as session:
                session.execute(stmt)
                session.commit()

            return True

        except Exception as e:
            logger.exception(f"Error klines insertion ({ticker}, {interval}): {e}")
            raise e
