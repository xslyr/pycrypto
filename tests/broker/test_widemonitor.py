import time

import pytest

from pycrypto.broker.binance import Broker
from pycrypto.broker.widemonitor import BinanceMonitor


@pytest.mark.binance_connection
def test_start_widemonitor(wait_for_condition):
    def data_arrived():
        return broker.widemonitor.monitor.size > 0

    broker = Broker(test_mode=True)
    assert broker.widemonitor is None

    broker.start_widemonitor()
    sucess = wait_for_condition(data_arrived)
    broker.stop_widemonitor()
    assert sucess, "Data not arrived on cache on time limit."
