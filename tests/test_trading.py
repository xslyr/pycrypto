import pytest

from pycrypto.commons.utils import DataSources
from pycrypto.trading import ItemRule, convert_data_to_numpy
from tests.broker_wrapper import BrokerWrapper

data = BrokerWrapper().get_klines("BTCUSDT", "1h", "2025-01-01 00:00:00")
data = convert_data_to_numpy(data, DataSources.mock)


@pytest.mark.parametrize(
    "key, operator, value, expected",
    [
        ("open", ">", 96165.29, False),
        ("open", ">=", 96165.29, True),
        ("open", "<", 96165.29, False),
        ("open", "<=", 96165.29, True),
        ("open", "!=", 96165.29, False),
        ("open", "==", 96165.29, True),
        ("close", ">", 95469.28, False),
        ("close", ">=", 95469.28, True),
        ("close", "<", 95469.28, False),
        ("close", "<=", 95469.28, True),
        ("close", "!=", 95469.28, False),
        ("close", "==", 95469.28, True),
        (None, ">", 95469.28, False),
        (None, ">=", 95469.28, True),
        (None, "<", 95469.28, False),
        (None, "<=", 95469.28, True),
        (None, "!=", 95469.28, False),
        (None, "==", 95469.28, True),
    ],
)
def test_itemrule_must_be_comparable_with_a_number(key, operator, value, expected):
    arr = ItemRule(data, key)  # noqa: F841
    assert eval(f"bool(arr {operator} {value})") == expected
