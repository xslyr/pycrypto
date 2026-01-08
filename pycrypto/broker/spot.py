import logging
import os
from operator import itemgetter
from typing import Any

import numpy as np
from binance.spot import Spot
from dotenv import load_dotenv

from pycrypto.commons.utils import BrokerUtils, Singleton, Timing

# https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Kline-Candlestick-Data

logger = logging.getLogger("app.spot")


class BinanceSpot(metaclass=Singleton):
    def __init__(self, test_mode=False):
        load_dotenv()
        self.test_mode = test_mode
        self._client = Spot(os.environ["BINANCE_APIKEY"], os.environ["BINANCE_SECRETKEY"])
        logger.info("BinanceSpot initializated.")

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
            raise e

    def klines(
        self,
        ticker: str,
        interval: str,
        start_time: Any,
        as_dict=False,
        limit: int = 1000,
    ) -> np.ndarray | list[dict]:
        try:
            adjusted_start_time = Timing.convert_any_to_timestamp(start_time)
            data = self._client.klines(
                symbol=ticker,
                interval=interval,
                startTime=adjusted_start_time,
                limit=limit,
            )
            cols = [
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
            dtypes = list(itemgetter(*cols)(BrokerUtils.ws_columns_dtype))

            if not as_dict:
                data_return = np.fromiter((tuple(row[:-1]) for row in data), dtype=dtypes)

            else:
                data_return = [dict(zip(cols, row)) for row in data]

            logger.info(f"Successful klines request of {ticker} on {interval} interval.")
            return data_return
        except Exception:
            logger.warning(f"Error on spot.klines. Ticker: {ticker}, Interval:{interval}")
            raise

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
            logger.debug(f"{buy_order=}")
            logger.info(f"Successful buy request for {quantity} of ticker {ticker}.")
            return True
        except Exception as e:
            logger.exception(f"Error on buy method. Ticker {ticker}, Qty: {quantity}, Type: {operation_type}")
            return e

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
            logger.debug(f"{sell_order=}")
            logger.info(f"Successful sell request for {quantity} of ticker {ticker}.")
            return True
        except Exception as e:
            return e
