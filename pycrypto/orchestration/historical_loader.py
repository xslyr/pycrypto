"""Module responsable to implements Loader class who can add historical klines into our database."""

import logging
import time as ts
from datetime import datetime, time
from typing import Any, Tuple

import numpy as np
from tqdm import tqdm

from pycrypto import db
from pycrypto.broker import Broker
from pycrypto.commons.utils import Timing, convert_any_to_datetime, get_timestamp_range_list

logger = logging.getLogger("app")


class Loader:
    """Class Loader for insert initial klines on database.

    Args:
        test_mode: bool for cascade variable for broker singleton
        database: optional parameter to share already database object
        broker: optional parameter to share already broker object

    """

    def __init__(self, test_mode=False, **kwargs):
        self.br: Broker = kwargs.get("broker", Broker(test_mode=test_mode))

    def __check_datetime_params(self, from_datetime: Any, between_datetimes: Any):
        """Wrapper for check datetime params.

        Args:
            from_datetime: single datetime
            between_datetimes: tuple of datetimes

        Returns:
            None

        """
        if from_datetime == "" and between_datetimes == ("", ""):
            raise Exception("Is necessary one of from_datetime or between_datetime param.")

        if from_datetime != "" and between_datetimes != ("", ""):
            raise Exception("Is necessary ONLY one of from_datetime or between_datetime param.")

    def __common_datetime_conversions(
        self, intervals, from_datetime, between_datetimes, verbose
    ) -> Tuple[datetime, datetime]:
        """Wrapper for common datetime convertions.

        Args:
            intervals: list of intervals the loader will work to.
            from_datetime: single datetime
            between_datetimes: tuple of datetimes
            verbose: bool to decide about logging

        Returns:
            tuple of start, end converted in datetime

        """
        start, end = None, None

        if from_datetime != "":
            start = convert_any_to_datetime(from_datetime)
            end = datetime.combine(datetime.now().date(), time(0, 0))
        else:
            start = convert_any_to_datetime(between_datetimes[0])
            end = convert_any_to_datetime(between_datetimes[1])

        if verbose:
            intv = ", ".join(intervals)
            logger.info(
                f"Loading Data from Binance to Database\n. Intervals ({intv})\tBetween datetimes ({start} e {end})\n"
            )

        return start, end

    def check_missing_data(
        self,
        ticker: str,
        intervals: list[str],
        from_datetime: Any = "",
        between_datetimes: tuple[Any, Any] = ("", ""),
        **kwargs,
    ) -> dict:
        """Method for check if there are missing data on database.

        Args:
            ticker: string of ticker coin
            intervals: list of intervals to check
            from_datetime: single datetime on any type
            between_datetimes: tuple of datetimes on any type
            kwargs:
                verbose: bool to decide about logging

        Returns:
            dict with keys interval and values with list of missing timestamps

        """
        verbose = kwargs.get("verbose", False)
        self.__check_datetime_params(from_datetime, between_datetimes)
        start, end = self.__common_datetime_conversions(intervals, from_datetime, between_datetimes, verbose)

        remaining_timestamps = {}
        for i in intervals[::-1]:
            timestamp_range = get_timestamp_range_list(start, end, i)
            timestamps_saved = [
                int(i[0] / 1000)
                for i in db.select_klines(
                    ticker, i, between_datetimes=(start, end), cols=["open_time"], returns="tuple"
                )
            ]

            remaining_timestamps[i] = np.setdiff1d(timestamp_range, timestamps_saved, assume_unique=True).tolist()

        return remaining_timestamps

    def dump_klines_into_db(
        self,
        ticker: str,
        intervals: list,
        from_datetime: Any = "",
        between_datetimes: tuple[Any, Any] = ("", ""),
        **kwargs,
    ) -> bool:
        """Method to insert klines into database

        Args:
            ticker: string of ticker coin
            intervals: list of intervals to check
            from_datetime: single datetime on any type
            between_datetimes: tuple of datetimes on any type
            kwargs:
                verbose: bool to decide about logging

        Returns:
            bool that show success or error of operation

        """
        try:
            verbose = kwargs.get("verbose", False)
            self.__check_datetime_params(from_datetime, between_datetimes)

            start, end = self.__common_datetime_conversions(intervals, from_datetime, between_datetimes, verbose)

            for i in intervals[::-1]:
                delta = Timing.delta_intervals[i]
                intervals_between_datetimes = int((end - start) / delta)
                full_loops, final_round = divmod(intervals_between_datetimes, 1000)
                if verbose:
                    process = tqdm(total=full_loops + 1, desc=f"Loading {i} interval ...", unit="unit")

                date_aux = start
                for _ in range(full_loops):
                    data = self.br.get_klines(ticker, i, date_aux)
                    db.insert_klines(ticker, i, data)

                    date_aux += 1000 * delta
                    if full_loops > 60:
                        ts.sleep(1)

                    if verbose:
                        process.update(1)

                data = self.br.get_klines(ticker, i, date_aux, as_dict=True, limit=final_round)
                db.insert_klines(ticker, i, data)
                if verbose:
                    process.update(1)

            return True

        except Exception:
            logger.exception("Error on loading binance data into db.")
            raise
