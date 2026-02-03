import time

import pytest

from pycrypto.broker.binance import Broker
from pycrypto.broker.widemonitor import BinanceMonitor


@pytest.mark.binance_connection
def test_start_widemonitor(wait_for_condition):
    def data_arrived():
        return bm.monitor.size > 0

    bm = BinanceMonitor()
    assert bm.monitor.size == 0

    bm.start_websocket()
    sucess = wait_for_condition(data_arrived)
    bm.close_websocket()
    assert sucess, "Data not arrived on cache on time limit."
