import pytest
from sqlalchemy.orm import attributes

from pycrypto.models.main import AppConfig, Bkline, Klines_1s


def test_app_config_init_behavior():
    """Valida que o ID é gerado pelo banco (init=False) e os campos obrigatórios."""
    # O campo 'id' tem init=False, logo não deve ser passado no construtor
    config = AppConfig(key="MIN_NOTIONAL", value="10.0")

    assert config.key == "MIN_NOTIONAL"
    assert config.value == "10.0"
    with pytest.raises(AssertionError) as e:
        _ = config.id
        assert "AssertionError" in str(e.value)


def test_kline_inheritance_and_data():
    """Valida se as classes concretas herdam corretamente de Bkline."""
    kline_data = {
        "ticker": "BTCUSDT",
        "open_time": 1700000000000,
        "open": 50000.0,
        "high": 51000.0,
        "low": 49000.0,
        "close": 50500.0,
        "base_asset_volume": 10.5,
        "close_time": 1700000059999,
        "quote_asset_volume": 530250.0,
        "number_of_trades": 1500,
        "taker_buy_base_asset_volume": 5.2,
        "taker_buy_quote_asset_volume": 262600.0,
    }

    k1s = Klines_1s(**kline_data)

    assert isinstance(k1s, Bkline)
    assert k1s.ticker == "BTCUSDT"
    assert k1s.open == 50000.0
    assert k1s.number_of_trades == 1500


def test_bkline_is_abstract():
    """Garante que a classe base Bkline não seja instanciada diretamente."""
    with pytest.raises(TypeError):
        # SQLAlchemy MappedAsDataclass impede instanciar classes abstratas ou incompletas
        _ = Bkline(ticker="TEST", open_time=123, open=1.0)
