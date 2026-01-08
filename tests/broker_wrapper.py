import json
import logging
import os
from pathlib import Path
from typing import Any

import numpy as np
from binance.error import ClientError

from pycrypto.broker import Broker

logger = logging.getLogger("app.spot")


class BrokerWrapper(Broker):
    """Wrapper do pycrypto.broker para adicionar a funcionalidade de mock necessária no Github Actions"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mock_path = Path("tests/mock")
        self.is_ci_env = os.getenv("GITHUB_ACTIONS") == "true"

    def get_klines(self, ticker: str, interval: str, start_time: Any, **kwargs) -> np.ndarray | list[dict]:
        mock_required = kwargs.get("mock_required", True)
        as_dict = kwargs.get("as_dict", True)
        filename = f"{ticker}_{interval}_{start_time}"
        filename += "_dict.json" if as_dict else "_as_array.json"
        mock_file = self.mock_path / filename

        if not mock_file.exists() and self.is_ci_env:
            raise FileNotFoundError(f"Mock files {mock_file} not fount! It's required for CI environment.")

        try:
            if mock_required and (self.is_ci_env or mock_file.exists()):
                with open(mock_file, "r") as f:
                    data = json.load(f)
                    return super().spot.convert_spotklines_to_numpy(data) if not as_dict else data

            data = super().get_klines(ticker, interval, start_time, as_dict=as_dict, **kwargs)
            self.mock_path.mkdir(parents=True, exist_ok=True)

            with open(mock_file, "w") as f:
                serializable_data = np.array(data).tolist() if hasattr(data, "tolist") else data
                json.dump(serializable_data, f)

            return data

        except Exception:
            logger.exception("Error on getklines of BrokerWrapper")
            raise

    def buy(self, ticker: str, quantity: int, operation_type: str = "MARKET"):
        params = [ticker, quantity, operation_type]
        return self.return_fake_error(ticker) if self.is_ci_env else super().buy(*params)

    def sell(self, ticker: str, quantity: int, operation_type: str = "MARKET"):
        params = [ticker, quantity, operation_type]
        return self.return_fake_error(ticker) if self.is_ci_env else super().sell(*params)

    def return_fake_error(self, ticker):
        params = {
            "status_code": 400,
            "error_code": -1121,
            "error_message": "Invalid symbol.",
            "header": {"Content-Type": "application/json;charset=UTF-8"},
        }
        return {} if ticker == "BTCUSDT" else ClientError(**params)

    @property
    def wallet(self, mock_required=True) -> dict:
        mock_file = self.mock_path / "wallet"

        if not mock_file.exists() and self.is_ci_env:
            raise FileNotFoundError(f"Mock files {mock_file} not fount! It's required for CI environment.")

        try:
            if mock_required and (self.is_ci_env or mock_file.exists()):
                with open(mock_file, "r") as f:
                    return json.load(f)

            data = super().wallet
            self.mock_path.mkdir(parents=True, exist_ok=True)

            with open(mock_file, "w") as f:
                json.dump(data, f)

            return data
        except Exception:
            logger.exception("Error on wallet of BrokerWrapper")
            raise

    def trade_fee(self, mock_required=True) -> dict:
        mock_file = self.mock_path / "trade_fee"

        if not mock_file.exists() and self.is_ci_env:
            raise FileNotFoundError(f"Mock files {mock_file} not fount! It's required for CI environment.")

        try:
            if mock_required and (self.is_ci_env or mock_file.exists()):
                with open(mock_file, "r") as f:
                    return json.load(f)

            data = super().trade_fee
            self.mock_path.mkdir(parents=True, exist_ok=True)

            with open(mock_file, "w") as f:
                json.dump(data, f)

            return data
        except Exception:
            logger.exception("Error on tradefee of BrokerWrapper")
            raise
