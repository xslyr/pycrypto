import numpy as np


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
