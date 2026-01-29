import pytest

from pycrypto.trading.strategy import Market, TradeStrategy


@pytest.mark.parametrize("key", ["wrong key", "wrong.key"])
def test_market_must_prevent_keys_with_space_and_dots(key):
    with pytest.raises(Exception) as error:
        Market({key: None})
        assert "Space and dots are not allowed for keys on Market objects." in str(error.value)


def test_market_must_be_able_concatenate_dict_keys(dict_test):
    m = Market(dict_test)
    assert isinstance(m.key1.key11, Market)


def test_market_must_return_keys(dict_test):
    m = Market(dict_test)
    assert m.items == list(dict_test.keys())
    assert m.key1.items == list(dict_test["key1"].keys())


def test_market_must_check_has_items(dict_test):
    m = Market(dict_test)
    assert m.has_items
    assert m.key1.has_items
    assert not m.key1.key12.has_items
    assert not m.key2.has_items


def test_tradestrategy_must_return_market_atribute_info(dict_test):
    m = Market(dict_test)
    t = TradeStrategy(m, m)
    assert t.on_up_market == m
    assert t.on_down_market == m
    assert t.on_side_market is None
