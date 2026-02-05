class PycryptoException(Exception):
    def __init__(self, msg):
        super().__init__(msg)


class DefaultException:
    interval_not_available = PycryptoException("Interval not available.")

    database_cleaning_kline_table = PycryptoException("Error on cleaning kline_table.")
    database_column_not_available = PycryptoException("Some column are not available.")
    database_insertion_kline = PycryptoException("Error on klines insertion.")

    utils_datetime_parameter_format = PycryptoException(
        "On datetime param we expect str with 19 chars. e.g. 2023-01-01 00:00:00 \nYou can also consider send timestamp or datetime obj param."
    )
    utils_unknown_timestamp_format = PycryptoException("Unknown timestamp format.")

    spot_connection = PycryptoException(
        "Error on binance connection. Please verify environment variables or internet connection."
    )
    spot_kline_request = PycryptoException("Error on binance spot request.")
    spot_buy_coin = PycryptoException("Error on buy coin.")
    spot_sell_coin = PycryptoException("Error on sell coin.")

    websocket_connection = PycryptoException("Error on starting websocket. Please verify your internet connection.")

    cache_stream_parameter_format = PycryptoException(
        "Stream parameter must be a tuple of (ticker,interval) or string like ticker@kline_interval."
    )
    cache_infostream = PycryptoException("Error on get_info_stream. Verify logs for details.")
    cache_deletion = PycryptoException("Error on deletion stream data.")

    orchestration_one_datetime_param_needed = PycryptoException(
        "Is necessary one of from_datetime or between_datetime param."
    )
    orchestration_only_one_datetime_param_allow = PycryptoException(
        "Is necessary ONLY one of from_datetime or between_datetime param."
    )

    test_wrapper_file_not_found = PycryptoException("Mock file not found! It's required for CI environment.")
    test_wrapper_getklines = PycryptoException("Error on getklines of BrokerWrapper")
    test_wrapper_wallet = PycryptoException("Error on wallet of BrokerWrapper")
    test_wrapper_tradefee = PycryptoException("Error on tradefee of BrokerWrapper")
