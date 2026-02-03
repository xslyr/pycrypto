import numpy as np

kline_columns = [
    "open_time",
    "close_time",
    "open",
    "close",
    "high",
    "low",
    "base_asset_volume",
    "quote_asset_volume",
    "number_of_trades",
    "taker_buy_base_asset_volume",
    "taker_buy_quote_asset_volume",
    "ignore",
]

ws_columns_names = {
    "open_time": "t",
    "close_time": "T",
    "ticker": "s",
    "interval": "i",
    "first_trade_id": "f",
    "last_trade_id": "L",
    "open": "o",
    "close": "c",
    "high": "h",
    "low": "l",
    "base_asset_volume": "v",
    "number_of_trades": "n",
    "is_kline_closed": "x",
    "quote_asset_volume": "q",
    "taker_buy_base_asset_volume": "V",
    "taker_buy_quote_asset_volume": "Q",
    "ignore": "B",
}

columns_dtype = {
    "open_time": ("open_time", "i8"),
    "close_time": ("close_time", "i8"),
    "ticker": ("ticker", "S10"),
    "interval": ("interval", "S3"),
    "first_trade_id": ("first_trade_id", "i8"),
    "last_trade_id": ("last_trade_id", "i8"),
    "open": ("open", "f8"),
    "close": ("close", "f8"),
    "high": ("high", "f8"),
    "low": ("low", "f8"),
    "base_asset_volume": ("base_asset_volume", "f8"),
    "number_of_trades": ("number_of_trades", "i8"),
    "is_kline_closed": ("is_kline_closed", "?"),
    "quote_asset_volume": ("quote_asset_volume", "f8"),
    "taker_buy_base_asset_volume": ("taker_buy_base_asset_volume", "f8"),
    "taker_buy_quote_asset_volume": ("taker_buy_quote_asset_volume", "f8"),
    "ignore": ("ignore", "?"),
}

websocket_opened_maxlen = {
    "1s": 60,
    "1m": 60,
    "3m": 20,
    "5m": 12,
    "15m": 4,
    "30m": 2,
    "1h": 24,
    "2h": 12,
    "4h": 6,
    "6h": 4,
    "8h": 3,
    "12h": 2,
    "1d": 1,
}

widemonitor_columns = {
    "e": "etype",  # Event type (tipo de evento).
    "E": "timestamp",  # Event time (tempo do evento) em milissegundos desde a Época Unix.
    "s": "ticker",  # Símbolo do par de negociação.
    "p": "var_price",  # Variação de preço (price change) no período.
    "P": "pct_price",  # Variação percentual do preço (price change percentage) no período.
    "w": "vwap",  # Preço médio ponderado no período.
    "x": "last_price",  # Preço do último trade realizado antes do fechamento do ticker.
    "c": "close",  # Preço de fechamento no período.
    "Q": "last_qty",  # Quantidade do último trade realizado.
    "b": "best_bid_price",  # O melhor preço de compra no topo do livro (Bid).
    "B": "best_bid_qty",  # A quantidade disponível no melhor preço de compra.
    "a": "best_ask_price",  # O melhor preço de venda no topo do livro (Ask).
    "A": "best_ask_qty",  # A quantidade disponível no melhor preço de venda.
    "o": "open",  # Preço de abertura no período.
    "h": "high",  # Preço mais alto no período.
    "l": "low",  # Preço mais baixo no período.
    "v": "base_asset_volume",  # Volume total negociado no período.
    "q": "quote_asset_volume",  # Volume total em dólares negociado no período.
    "O": "open_time",  # Timestamp da abertura no período apresentado.
    "C": "close_time",  # Timestamp de fechamento no período apresentado.
    "F": "first_trade",  # Primeiro trade (primeiro negócio) no período.
    "L": "last_trade",  # Último trade (último negócio) no período.
    "n": "number_of_trades",  # Número total de trades no período.
}

widemonitor_columns_dtype = {
    "etype": ("etype", "U8"),
    "timestamp": ("timestamp", "i8"),
    "ticker": ("ticker", "U15"),
    "pct_price": ("pct_price", "f8"),
    "var_price": ("var_price", "f8"),
    "vwap": ("vwap", "f8"),
    "last_price": ("last_price", "f8"),
    "close": ("close", "f8"),
    "last_qty": ("last_qty", "f8"),
    "best_bid_price": ("best_bid_price", "f8"),
    "best_bid_qty": ("best_bid_qty", "f8"),
    "best_ask_price": ("best_ask_price", "f8"),
    "best_ask_qty": ("best_ask_qty", "f8"),
    "open": ("open", "f8"),
    "high": ("high", "f8"),
    "low": ("low", "f8"),
    "base_asset_volume": ("base_asset_volume", "f8"),
    "quote_asset_volume": ("quote_asset_volume", "f8"),
    "open_time": ("open_time", "f8"),
    "close_time": ("close_time", "f8"),
    "first_trade": ("first_trade", "i8"),
    "last_trade": ("last_trade", "i8"),
    "number_of_trades": ("number_of_trades", "i8"),
}


def upsert_monitor_arr(storage, new_batch):
    if storage.size == 0:
        combined = new_batch
    else:
        combined = np.concatenate([storage, new_batch])

    combined.sort(order=["ticker", "timestamp"])

    mask = np.empty(combined.size, dtype=bool)
    mask[:-1] = combined["ticker"][:-1] != combined["ticker"][1:]
    mask[-1] = True

    return combined[mask]


def aggregate_structured(arr, group_key, agg_dict):
    uniques = np.unique(arr[group_key])

    res_dtype = [(group_key, arr.dtype[group_key])] + [(col, arr.dtype[col]) for col in agg_dict.keys()]
    result = np.empty(uniques.size, dtype=res_dtype)

    for i, val in enumerate(uniques):
        mask = arr[group_key] == val
        subset = arr[mask]

        result[i][group_key] = val
        for col, func in agg_dict.items():
            result[i][col] = func(subset[col])

    return result


def ticker_contains(data: np.ndarray, ticker_filter: str = "USDT"):
    mask = np.char.find(data["ticker"], ticker_filter)
    return data[mask] if mask != -1 else np.array([], dtype=data.dtype)
