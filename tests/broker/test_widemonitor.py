import time

import pytest

from pycrypto.broker.broker import Broker
from pycrypto.broker.widemonitor import BinanceMonitor


@pytest.mark.binance_websocket
def test_start_widemonitor():
    bm = BinanceMonitor()
    assert bm.monitor.size == 0
    bm.start_websocket()
    time.sleep(3)
    assert bm.monitor.size > 0
    bm.close_websocket()
