import pytest

from pycrypto import db
from pycrypto.commons.utils import Singleton, convert_data_to_numpy
from tests.broker_wrapper import BrokerWrapper


@pytest.fixture(autouse=True)
def reset_singletons():
    yield
    Singleton._reset_all()


@pytest.fixture
def broker():
    return BrokerWrapper(test_mode=True)


@pytest.fixture
def numpy_data():
    data = BrokerWrapper(test_mode=True).get_klines("BTCUSDT", "1h", "2025-01-01 00:00:00")
    return convert_data_to_numpy(data)


@pytest.fixture
def dict_test():
    return {"key1": {"key11": {"key111": []}, "key12": {}}, "key2": {}}


@pytest.fixture
def cleaned_1d_table_scenario(broker):
    data = broker.get_klines("BTCUSDT", "1d", "2025-01-01 00:00:00")
    db.clean_kline_table(["1d"])
    db.insert_klines("BTCUSDT", "1d", data)
    return data
