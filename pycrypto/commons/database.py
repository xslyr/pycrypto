import logging
import os
from typing import Any, Tuple

import numpy as np
import pandas as pd
import psycopg
from psycopg import Connection, OperationalError, sql

from .utils import BrokerUtils, DatabaseDescription, Singleton, Timing

logger = logging.getLogger("app")


class Database(metaclass=Singleton):
    def __init__(self, **kwargs):
        host = kwargs.get("host", os.getenv("POSTGRES_HOST"))
        port = kwargs.get("port", os.getenv("POSTGRES_PORT"))
        dbname = kwargs.get("dbname", os.getenv("POSTGRES_DB"))
        user = kwargs.get("user", os.getenv("POSTGRES_USER"))
        password = kwargs.get("password", os.getenv("POSTGRES_PASSWORD"))
        connection_string = f"dbname={dbname} user={user} password={password} host={host} port={port}"
        try:
            self._conn = psycopg.connect(connection_string)
            self._conn.autocommit = True
        except OperationalError:
            logger.exception("Database connection error. Please verify environment variables and service availability.")
            raise

    @property
    def conn(self) -> Connection:
        return self._conn

    def rollback(self):
        self.conn.rollback()

    def check_fix_db_integrity(self):
        try:
            cur = self.conn.cursor()
            select_result = cur.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'AND table_type = 'BASE TABLE'; "
            ).fetchall()

            if len(select_result) < DatabaseDescription.qty_tables:
                db_structure = DatabaseDescription.db_structure

                sql_base_create = "CREATE TABLE IF NOT EXISTS {} ({}, {});"
                for table, table_structure in db_structure.items():
                    columns = " , ".join(
                        map(
                            lambda x: " ".join(x),
                            table_structure["columns"].items(),
                        )
                    )
                    primary_key = table_structure["primary_key"]
                    if table == "klines_tables":
                        for kline in Timing.intervals_available:
                            table_name = f"klines_{kline}"
                            create_command = sql.SQL(sql_base_create).format(
                                sql.Identifier(table_name),
                                sql.SQL(columns),
                                sql.SQL(primary_key),
                            )
                            cur.execute(create_command)

                    else:
                        create_command = sql.SQL(sql_base_create).format(
                            sql.Identifier(table),
                            sql.SQL(columns),
                            sql.SQL(primary_key),
                        )
                        cur.execute(create_command)
            return True

        except Exception:
            self.conn.rollback()
            logger.exception("Error on check_fix_db_integrity. ")
            raise

    def destroy_all_tables(self, confirmation=False):
        try:
            if confirmation:
                cur = self.conn.cursor()
                select_result = cur.execute(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'AND table_type = 'BASE TABLE'; "
                ).fetchall()

                for line in select_result:
                    drop_command = sql.SQL("DROP TABLE IF EXISTS {} CASCADE;").format(sql.Identifier(line[0]))
                    cur.execute(drop_command)

                return True
            return False
        except Exception:
            logger.exception("Error on destroy_all_tables.")
            raise

    def clean_kline_table(self, intervals: list[str]):
        try:
            cur = self.conn.cursor()
            for i in intervals:
                table = f"klines_{i}"
                query = sql.SQL("delete from {}").format(sql.Identifier(table))
                cur.execute(query)
            return True
        except Exception:
            self.conn.rollback()
            logger.exception("Error on clen_kline_table.")
            raise

    def select_klines(
        self,
        ticker: str,
        interval: str,
        from_datetime: Any = "",
        between_datetimes: tuple[Any, Any] = ("", ""),
        **kwargs,
    ) -> list[Tuple]:
        only_columns: list[str] = kwargs.get("only_columns", BrokerUtils.kline_columns[2:-1])
        columns = sql.SQL(", ").join(map(sql.Identifier, only_columns))
        query, params = "", ""

        if from_datetime == "" and between_datetimes == ("", ""):
            raise Exception("Is necessary one of from_datetime or between_datetime param.")

        if from_datetime != "" and between_datetimes != ("", ""):
            raise Exception("Is necessary ONLY one of from_datetime or between_datetime param.")

        table = f"klines_{interval}"
        if from_datetime != "":
            query = sql.SQL("SELECT {} FROM {} WHERE ticker=%s AND open_time >= %s ").format(
                columns, sql.Identifier(table)
            )
            params = (ticker, Timing.convert_any_to_timestamp(from_datetime))

        if all(between_datetimes):
            query = sql.SQL("SELECT {} FROM {} WHERE ticker=%s AND open_time between %s and %s ").format(
                columns, sql.Identifier(table)
            )
            params = (
                ticker,
                Timing.convert_any_to_timestamp(between_datetimes[0]),
                Timing.convert_any_to_timestamp(between_datetimes[1]),
            )

        cur = self.conn.cursor()
        return cur.execute(query, params).fetchall()

    def insert_klines(self, ticker: str, interval: str, data: list) -> bool:
        try:
            table = f"klines_{interval}"
            columns = DatabaseDescription.db_structure["klines_tables"]["columns"].keys()

            column_names = sql.SQL(", ").join(sql.Identifier(c) for c in columns)
            placeholders = sql.SQL(", ").join(sql.Placeholder() * len(columns))

            base_sql = "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT (ticker, open_time) DO NOTHING;"
            query = sql.SQL(base_sql).format(sql.Identifier(table), column_names, placeholders)

            limit = len(data)
            arr1 = np.array([ticker] * limit).reshape(-1, 1)
            arr2 = np.array(data)[:, :-1]

            data_to_insert = pd.DataFrame(np.hstack((arr1, arr2))).to_records(index=False).tolist()

            cur = self.conn.cursor()
            cur.executemany(query, data_to_insert)

            return True

        except Exception:
            self.conn.rollback()
            logger.exception(f"Error on inserting klines, ticker {ticker}, interval {interval}, data: {data}")
            raise
