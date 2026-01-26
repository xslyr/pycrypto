import pytest

from pycrypto.commons.cache import Cache
from pycrypto.commons.database import Database
from pycrypto.commons.utils import DataSources, Singleton
from pycrypto.trading import convert_data_to_numpy
from tests.broker_wrapper import BrokerWrapper


@pytest.fixture(autouse=True)
def reset_singletons():
    Cache()
    Database().check_fix_db_integrity()
    yield
    Singleton._reset_all()


@pytest.fixture
def broker():
    return BrokerWrapper(test_mode=True)


@pytest.fixture
def numpy_data():
    data = BrokerWrapper(test_mode=True).get_klines("BTCUSDT", "1h", "2025-01-01 00:00:00")
    return convert_data_to_numpy(data, DataSources.mock)


@pytest.fixture
def dict_test():
    return {"key1": {"key11": {"key111": []}, "key12": {}}, "key2": {}}
