from datetime import datetime, timedelta

import pytest

from pycrypto.commons.database import Database
from pycrypto.orchestration import Loader
from tests.broker_wrapper import BrokerWrapper

# TODO: implements the tests of data orchestrator

cbn = BrokerWrapper(test_mode=True)


def test_loader_check_missing_data_must_return_a_dict():
    loader = Loader()
    result = loader.check_missing_data("BTCUSDT", ["1h"], from_datetime="2025-01-01 00:00:00")
    assert isinstance(result, dict)


@pytest.mark.delete_db_data
def test_broker_loader_must_dump_data_in_kline_tables():
    cbd = Database()
    cbd.clean_kline_table(["1d"])

    d_inicio = datetime(2025, 1, 1)
    d_fim = datetime(2025, 1, 31)
    qty_intervals = int((d_fim - d_inicio) / timedelta(days=1))

    Loader(broker=cbn).dump_klines_into_db("BTCUSDT", ["1d"], between_datetimes=(d_inicio, d_fim), verbose=True)
    qty_records = len(cbd.select_klines("BTCUSDT", "1d", between_datetimes=(d_inicio, d_fim)))
    assert qty_records == qty_intervals
