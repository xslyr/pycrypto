import json
import logging
from operator import itemgetter

import numpy as np
from binance.websocket.websocket_client import BinanceWebsocketClient

from pycrypto.broker.nputils import upsert_monitor_arr
from pycrypto.commons.utils import BrokerUtils, Singleton

logger = logging.getLogger("app.websocket")


class BinanceMonitor(metaclass=Singleton):
    """
    All Market Tickers Streams - https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/All-Market-Tickers-Streams
    wss://stream.binance.com:9443/ws/!ticker@arr

    """

    _stream: BinanceWebsocketClient = None  # type:ignore

    def __init__(self):
        self.__base_url = "wss://stream.binance.com:9443/ws/"
        self.__subscription = "!ticker@arr"
        self.__monitor_cols = list(BrokerUtils.widemonitor_columns)[1:]
        self.__monitor_dtypes = list(BrokerUtils.widemonitor_columns_dtype.values())[1:]
        self.monitor: np.ndarray = np.empty(0, dtype=self.__monitor_dtypes)

    @property
    def stream(self):
        return self._stream

    def on_single_message(self, _, message):
        try:
            logger.debug(message)
            msg = json.loads(message)
            if isinstance(msg, list):
                arr = np.fromiter((itemgetter(*self.__monitor_cols)(row) for row in msg), dtype=self.__monitor_dtypes)
                self.monitor = upsert_monitor_arr(self.monitor, arr)
        except Exception as e:
            logger.info(e)
            print(f"{msg}\n\n")

    def pong(self, _, message):
        self._stream.subscribe(self.__subscription, id=message.decode("utf-8"))

    def on_error(self, _, message):
        logger.error(message)

    def close_websocket(self):
        self._stream.stop()

    def start_websocket(self) -> bool:
        try:
            url = self.__base_url + self.__subscription
            self._stream = BinanceWebsocketClient(
                stream_url=url,
                on_message=self.on_single_message,
                on_ping=self.pong,
                on_error=self.on_error,
            )
            return True
        except Exception:
            logger.exception("Error on starting wide monitor.")
            raise
