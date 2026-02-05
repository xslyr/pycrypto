import socket
from datetime import datetime

import numpy as np
import pytest

from pycrypto.broker.binance import Broker
from pycrypto.broker.spot import BinanceSpot
from pycrypto.commons.exception import DefaultException


@pytest.mark.binance_connection
def test_wallet_must_return_dict():
    broker = Broker(test_mode=True)
    wallet = broker.wallet
    assert isinstance(wallet, dict)


@pytest.mark.binance_connection
def test_tradefee_must_have_more_than_3000_items():
    broker = Broker(test_mode=True)
    trade_fee = broker.trade_fee
    # this assert condition prevent error on cases of few coins disabled
    assert len(trade_fee.keys()) > 3000  # pyright: ignore[reportAttributeAccessIssue]


@pytest.mark.parametrize(
    "start_time",
    ["2023-01-01 00:00:00", datetime(2023, 1, 1), 1672542000.0, 1672542000],
)
def test_getklines_must_accept_any_datetime_params(broker, start_time):
    params = {"ticker": "BTCUSDT", "interval": "1m", "limit": 1000}
    result = broker.get_klines(**params, start_time=start_time)
    assert len(result) == 1000


@pytest.mark.binance_connection
def test_getklines_must_return_dictvalues():
    broker = Broker()
    params = {"ticker": "BTCUSDT", "interval": "1m", "limit": 1000}
    result = broker.get_klines(**params, start_time="2023-01-01 00:00:00", as_dict=True)
    assert isinstance(result[0], dict)


def test_buy_must_return_valid_and_invalid_data(broker):
    buy1 = broker.buy(ticker="BTCUSDT", operation_type="MARKET", quantity=1)
    assert buy1 == {}
    with pytest.raises(Exception) as error:
        broker.buy(ticker="XXXXXX", operation_type="MARKET", quantity=1)
        assert DefaultException.spot_buy_coin == str(error.value)


def test_sell_must_return_valid_and_invalid_data(broker):
    sell1 = broker.sell(ticker="BTCUSDT", operation_type="MARKET", quantity=1)
    assert sell1 == {}
    with pytest.raises(Exception) as error:
        broker.sell(ticker="XXXXXX", operation_type="MARKET", quantity=1)
        assert DefaultException.spot_sell_coin == str(error.value)


@pytest.mark.binance_connection
def test_tradefee_must_return_exception_without_network(offline_network):
    with pytest.raises(Exception) as error:
        Broker(test_mode=True).trade_fee
        assert DefaultException.spot_connection == str(error.value)


@pytest.mark.binance_connection
def test_klines_must_return_as_numpy(broker):
    params = {"ticker": "BTCUSDT", "interval": "1d", "start_time": "2025-01-01 00:00:00", "as_dict": False}
    result = broker.spot.klines(**params)
    assert isinstance(result, np.ndarray)
