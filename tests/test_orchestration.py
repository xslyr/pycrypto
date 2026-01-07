from pycrypto.orchestration import Loader

# TODO: implements the tests of data orchestrator


def test_loader_check_missing_data_must_return_a_dict():
    loader = Loader()
    result = loader.check_missing_data("BTCUSDT", ["1h"], from_datetime="2025-01-01 00:00:00")
    assert isinstance(result, dict)
