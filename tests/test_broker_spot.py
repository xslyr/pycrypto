from datetime import datetime

import pytest
from binance.error import ClientError

from pycrypto.broker import Broker

cbn = Broker(test_mode=True)


@pytest.mark.binance_request
def test_wallet_must_return_dict():
    assert isinstance(cbn.wallet, dict)


@pytest.mark.binance_request
def test_tradefee_must_have_more_than_3000_items():
    # this assert condition prevent error on cases of few coins disabled
    assert len(cbn.trade_fee.keys()) > 3000  # pyright: ignore[reportAttributeAccessIssue]


@pytest.mark.binance_request
@pytest.mark.parametrize(
    "start_time",
    ["2023-01-01 00:00:00", datetime(2023, 1, 1), 1672542000.0, 1672542000],
)
def test_getklines_must_accept_any_datetime_params(start_time):
    params = {"ticker": "BTCUSDT", "interval": "1m", "limit": 1000}
    result = cbn.get_klines(**params, start_time=start_time)
    assert len(result) == 1000


@pytest.mark.binance_request
def test_getklines_must_return_dictvalues():
    params = {"ticker": "BTCUSDT", "interval": "1m", "limit": 1000}
    result = cbn.get_klines(**params, start_time="2023-01-01 00:00:00", as_dict=True)
    assert isinstance(result[0], dict)


@pytest.mark.binance_request
def test_buy_must_return_valid_and_invalid_data():
    assert cbn.buy(ticker="BTCUSDT", operation_type="MARKET", quantity=1)
    r = cbn.buy(ticker="XXXXXX", operation_type="MARKET", quantity=1)
    assert isinstance(r, ClientError)
    assert r.error_message == "Invalid symbol."


@pytest.mark.binance_request
def test_sell_must_return_valid_and_invalid_data():
    assert cbn.sell(ticker="BTCUSDT", operation_type="MARKET", quantity=1)
    r = cbn.sell(ticker="XXXXXX", operation_type="MARKET", quantity=1)
    assert isinstance(r, ClientError)
    assert r.error_message == "Invalid symbol."
