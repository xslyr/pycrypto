from datetime import datetime

import pytest
from binance.error import ClientError


def test_wallet_must_return_dict(broker):
    wallet = broker.wallet
    assert isinstance(wallet, dict)


def test_tradefee_must_have_more_than_3000_items(broker):
    trade_fee = broker.trade_fee()
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


def test_getklines_must_return_dictvalues(broker):
    params = {"ticker": "BTCUSDT", "interval": "1m", "limit": 1000}
    result = broker.get_klines(**params, start_time="2023-01-01 00:00:00", as_dict=True)
    assert isinstance(result[0], dict)


def test_buy_must_return_valid_and_invalid_data(broker):
    buy1 = broker.buy(ticker="BTCUSDT", operation_type="MARKET", quantity=1)
    assert buy1 == {}
    buy2 = broker.buy(ticker="XXXXXX", operation_type="MARKET", quantity=1)
    assert isinstance(buy2, ClientError)
    assert buy2.error_message == "Invalid symbol."


def test_sell_must_return_valid_and_invalid_data(broker):
    sell1 = broker.sell(ticker="BTCUSDT", operation_type="MARKET", quantity=1)
    assert sell1 == {}
    sell2 = broker.sell(ticker="XXXXXX", operation_type="MARKET", quantity=1)
    assert isinstance(sell2, ClientError)
    assert sell2.error_message == "Invalid symbol."
