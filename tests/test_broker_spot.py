from datetime import datetime

import pytest
from binance.error import ClientError

from tests.broker_wrapper import BrokerWrapper

cbn = BrokerWrapper(test_mode=True)


def test_wallet_must_return_dict():
    wallet = cbn.wallet
    assert isinstance(wallet, dict)


def test_tradefee_must_have_more_than_3000_items():
    trade_fee = cbn.trade_fee()
    # this assert condition prevent error on cases of few coins disabled
    assert len(trade_fee.keys()) > 3000  # pyright: ignore[reportAttributeAccessIssue]


@pytest.mark.parametrize(
    "start_time",
    ["2023-01-01 00:00:00", datetime(2023, 1, 1), 1672542000.0, 1672542000],
)
def test_getklines_must_accept_any_datetime_params(start_time):
    params = {"ticker": "BTCUSDT", "interval": "1m", "limit": 1000}
    result = cbn.get_klines(**params, start_time=start_time)
    assert len(result) == 1000


def test_getklines_must_return_dictvalues():
    params = {"ticker": "BTCUSDT", "interval": "1m", "limit": 1000}
    result = cbn.get_klines(**params, start_time="2023-01-01 00:00:00", as_dict=True)
    assert isinstance(result[0], dict)


def test_buy_must_return_valid_and_invalid_data():
    buy1 = cbn.buy(ticker="BTCUSDT", operation_type="MARKET", quantity=1)
    assert buy1 == {}
    buy2 = cbn.buy(ticker="XXXXXX", operation_type="MARKET", quantity=1)
    assert isinstance(buy2, ClientError)
    assert buy2.error_message == "Invalid symbol."


def test_sell_must_return_valid_and_invalid_data():
    sell1 = cbn.sell(ticker="BTCUSDT", operation_type="MARKET", quantity=1)
    assert sell1 == {}
    sell2 = cbn.sell(ticker="XXXXXX", operation_type="MARKET", quantity=1)
    assert isinstance(sell2, ClientError)
    assert sell2.error_message == "Invalid symbol."
