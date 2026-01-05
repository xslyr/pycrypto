
from app.broker import Broker

import pytest
import time as t
from binance.error import ClientError
from datetime import datetime
from icecream import ic


@pytest.mark.broker
def test_wallet():
    assert isinstance( Broker(test_mode=True).wallet, dict )


@pytest.mark.broker
def test_trade_fee():
    # this assert condition prevent error on cases of few coins disabled
    assert len( Broker(test_mode=True).trade_fee.keys() ) > 3000  # pyright: ignore[reportAttributeAccessIssue]


@pytest.mark.broker
def test_get_klines():
    cbn = Broker(test_mode=True)
    params = { 'ticker': 'BTCUSDT', 'interval': '1m', 'limit': 1000 }
    _datetime = datetime(2023, 1, 1) # timestamp = 1672542000.0
    
    ic('Testing get_klines ... with str params')
    assert len( cbn.get_klines(**params, start_time='2023-01-01 00:00:00') ) == 1000
    t.sleep(0.5)

    ic('Testing get_klines ... with datetime params')
    assert len( cbn.get_klines(**params, start_time=_datetime) ) == 1000
    t.sleep(0.5)

    ic('Testing get_klines ... with float params')
    assert len( cbn.get_klines(**params, start_time=1672542000.0) ) == 1000
    t.sleep(0.5)

    ic('Testing get_klines ... with int params')
    assert len( cbn.get_klines(**params, start_time=1672542000) ) == 1000
    t.sleep(0.5)

    ic('Testing get_klines ... with dict return true')
    assert isinstance( cbn.get_klines(**params, start_time='2023-01-01 00:00:00', as_dict=True)[0], dict)
    t.sleep(0.5)

@pytest.mark.broker
def test_buy():
    br = Broker(test_mode=True)
    assert br.buy( ticker='BTCUSDT', type='MARKET', quantity=1 ) == True
    r = br.buy( ticker='XXXXXX', type='MARKET', quantity=1 )
    assert isinstance( r, ClientError) 
    assert r.error_message == 'Invalid symbol.'


@pytest.mark.broker
def test_sell():
    br = Broker(test_mode=True)
    assert br.sell( ticker='BTCUSDT', type='MARKET', quantity=1) == True
    r = br.sell( ticker='XXXXXX', type='MARKET', quantity=1 )
    assert isinstance( r, ClientError) 
    assert r.error_message == 'Invalid symbol.'




