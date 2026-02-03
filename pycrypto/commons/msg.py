class Info:
    spot_started = "BinanceSpot initializated."
    websocket_start = "Starting websocket for ticker {} with intervals {}."
    websocket_string_conn = "Generated string connection: {}"


class Sucess:
    buy_coin = "Successful buy request."
    sell_coin = "Successful sell request."


class Error:
    binance_connection = "Error on binance connection. Please verify environment variables or internet connection."
    websocket_connection = "Error on starting websocket. Please verify your internet connection."
    binance_kline_request = "Error on binance spot request."
    buy_coin = "Error on buy coin."
    sell_coin = "Error on sell coin."


class Message:
    info = Info
    sucess = Sucess
    error = Error
