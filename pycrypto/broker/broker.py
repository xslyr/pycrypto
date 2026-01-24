import logging
from typing import Any

import numpy as np

from pycrypto.broker.websocket import BinanceWebsocket
from pycrypto.broker.widemonitor import BinanceMonitor
from pycrypto.commons import Cache, Database
from pycrypto.commons.utils import Singleton

from .spot import BinanceSpot

logger = logging.getLogger("app.spot")


class Broker(metaclass=Singleton):
    """Singleton wrapper class for interface spot and websocket methods.

    Args:
        test_mode: variable to cascade for broker who define if buy and sell methods will be fake or not.

    """

    def __init__(self, test_mode: bool = False):
        try:
            Database()
        except Exception as e:
            logger.warning(e)

        try:
            Cache()
        except Exception as e:
            logger.warning(e)

        self.test_mode = test_mode
        try:
            self.spot = BinanceSpot(test_mode)
        except Exception:
            logger.exception("Error on binance connection. Please verify environment variables or internet")
        self.websocket: BinanceWebsocket
        self._trade_fee = None

    @property
    def wallet(self) -> dict:
        """Method to bring info about coins on binance wallet.

        Args:
            None

        Returns:
            A dictionary with coins on keys and infos in values.

        """
        return self.spot.wallet()

    @property
    def trade_fee(self) -> dict:
        """Method to bring info about pair-assets involved and their trade fee.

        Args:
            None

        Returns:
            A dictionary with pair on keys and infos in values.

        """
        if self._trade_fee is None:
            self._trade_fee = self.spot.trade_fee

        return self._trade_fee()

    def start_websocket(self, ticker="BTCUSDT", intervals=["1s", "1m", "1h"]):
        """Method to start websocket receiving klines.

        Args:
            ticker: pair-coin who will be monitored.
            intervals: list of intervals who will be monitored.
            !!! The websocket return data saves directly on redis cache with stream name like a:
            - {ticker}@klines_{interval}:closed
            - {ticker}@klines_{interval}:opened

        Returns:
            Boolean indicating the success or failure of the execution.

        """
        try:
            self.websocket = BinanceWebsocket(ticker, intervals)
            self.websocket.start_websocket()
            return True
        except Exception:
            logger.exception("Error on websocket initialization.")
            return False

    def stop_websocket(self):
        """Method to stop websocket info receiving.

        Args:
            None

        Rerturns:
            Boolean indicating the success or failure of the execution.

        """
        try:
            self.websocket.close_websocket()
            return True
        except Exception:
            logger.exception("Error on close websocket")
            return False

    def get_klines(
        self,
        ticker: str,
        interval: str,
        start_time: Any,
        as_dict=True,
        limit: int = 1000,
    ) -> np.ndarray | list[dict]:
        """Method to get past klines infos via spot request.

        Args:
            ticker: coin who will be consulted
            interval: interval who will be consulted
            start_time: first period of klines will be returned
            as_dict: this parameter define if result will be a list of dictionary. if false the result is a list of tuples.
            limit: max number of periods returned by binance spot instance. The default maximum number is 1000.

        Returns:
            A list of tuple or list of dicts, depending the value of as_dict parameter.

        """
        return self.spot.klines(ticker, interval, start_time, as_dict, limit)

    def buy(self, ticker: str, quantity: int, operation_type: str = "MARKET"):
        """Method to make buy of coin.

        Args:
            ticker: coin who will negotiated.
            quantity: quantity of base asset that will be negotiated.
            type: type of order who will be requested.

        Returns:
            A dictionary containing info about order requested. If broker is on test_mode, this dictionary will be empty.

        """
        return self.spot.buy(ticker, quantity, operation_type)

    def sell(self, ticker: str, quantity: int, operation_type: str = "MARKET"):
        """Method to make sell of coin.

        Args:
            ticker: coin who will negotiated.
            quantity: quantity of base asset that will be negotiated.
            type: type of order who will be requested.

        Returns:
            A dictionary containing info about order requested. If broker is on test_mode, this dictionary will be empty.

        """
        return self.spot.sell(ticker, quantity, operation_type)

    def start_widemonitor(self):
        """Method to start widemonitor.

        Args:
            None.

        Returns:
            Boolean indicating the success or failure of the execution.
        """
        try:
            self.widemonitor = BinanceMonitor()
            self.widemonitor.start_websocket()
            return True
        except Exception:
            logger.exception("Error on initialization o widemonitor.")
            return False

    def stop_widemonitor(self):
        """Method to stop widemonitor.

        Args:
            None

        Returns:
            Boolean indicating the success or failure of the execution.
        """
        try:
            self.widemonitor.close_websocket()
            return True
        except Exception:
            logger.exception("Error on stoping widemonitor.")
            return False
