from app.data_orchestration import Loader
from datetime import datetime

# TODO: implementar test data orchestrator



def test_loader_check_missing_data():
    l = Loader()

    assert isinstance(l.check_missing_data('BTCUSDT',['1h'],from_datetime='2025-01-01 00:00:00'), dict)

    