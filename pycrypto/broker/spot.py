import logging
import os
from operator import itemgetter
from typing import Any, Tuple

import numpy as np
from binance.spot import Spot

from pycrypto.broker.utils import columns_dtype
from pycrypto.commons.msg import Error, Message
from pycrypto.commons.utils import Singleton, convert_any_to_timestamp

# https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Kline-Candlestick-Data

logger = logging.getLogger("app.spot")


class BinanceSpot(metaclass=Singleton):
    spot_cols = [
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "base_asset_volume",
        "close_time",
        "quote_asset_volume",
        "number_of_trades",
        "taker_buy_base_asset_volume",
        "taker_buy_quote_asset_volume",
    ]

    def __init__(self, test_mode=False):
        self.test_mode = test_mode
        self._client = Spot(os.environ["BINANCE_APIKEY"], os.environ["BINANCE_SECRETKEY"])
        logger.info(Message.info.spot_started)

    def wallet(self):
        wallet = {dc["asset"]: dc["free"] for dc in self._client.user_asset()}
        logger.debug(f"{wallet=}")
        return wallet

    def trade_fee(self):
        try:
            fee = self._client.trade_fee()
            info = self._client.exchange_info()
            trade_fee = {
                d["symbol"]: {
                    "buyerFee": d["takerCommission"],
                    "sellerFee": d["makerCommission"],
                }
                for d in fee
            }
            for i in info["symbols"]:
                if trade_fee.get(i["symbol"]):
                    trade_fee[i["symbol"]].update({"baseAsset": i["baseAsset"], "quoteAsset": i["quoteAsset"]})
            logger.debug(f"{trade_fee=}")
            return trade_fee
        except Exception as e:
            logger.exception(e)
            raise Exception(Error.websocket_connection)

    def convert_spotklines_to_numpy(self, data: list[Tuple]) -> np.ndarray:
        dtypes = list(itemgetter(*self.spot_cols)(columns_dtype))
        return np.fromiter((tuple(row[:-1]) for row in data), dtype=dtypes)

    def klines(
        self,
        ticker: str,
        interval: str,
        start_time: Any,
        as_dict=False,
        limit: int = 1000,
    ) -> np.ndarray | list[dict]:
        try:
            adjusted_start_time = convert_any_to_timestamp(start_time)
            data = self._client.klines(
                symbol=ticker,
                interval=interval,
                startTime=adjusted_start_time,
                limit=limit,
            )
            if not as_dict:
                data_return = self.convert_spotklines_to_numpy(data)

            else:
                data_return = [dict(zip(self.spot_cols, row)) for row in data]

            logger.debug(Message.sucess.buy_coin)
            return data_return
        except Exception as e:
            logger.warning(e)
            raise Exception(Message.error.binance_kline_request)

    def buy(self, ticker: str, quantity: int, operation_type="MARKET"):
        try:
            params = {
                "symbol": ticker,
                "side": "BUY",
                "type": operation_type,
                "quantity": quantity,
            }
            if self.test_mode:
                buy_order = self._client.new_order_test(**params)
            else:
                buy_order = self._client.new_order(**params)

            logger.info(Message.sucess.buy_coin)
            return buy_order
        except Exception as e:
            logger.exception(e)
            return Exception(Message.error.buy_coin)

    def sell(self, ticker: str, quantity: int, operation_type="MARKET"):
        try:
            params = {
                "symbol": ticker,
                "side": "SELL",
                "type": operation_type,
                "quantity": quantity,
            }
            if self.test_mode:
                sell_order = self._client.new_order_test(**params)
            else:
                sell_order = self._client.new_order(**params)

            logger.info(Message.sucess.sell_coin)
            return sell_order
        except Exception as e:
            logger.warning(e)
            return Exception(Message.error.sell_coin)
