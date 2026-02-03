from operator import itemgetter
from typing import Dict, Tuple

import numpy as np

from pycrypto.broker.utils import columns_dtype, kline_columns, ws_columns_names


def convert_data_to_numpy(data: list[Dict | Tuple], from_websocket=False, **kwargs) -> np.array:
    """Method to prepare and convert crude data to numpy array"""
    result = None
    if from_websocket:
        cols = kwargs.get("cols", kline_columns[2:-1])
        dtypes = list(itemgetter(*cols)(columns_dtype))
        col_ids = itemgetter(*cols)(ws_columns_names)
        arr = (itemgetter(*col_ids)(i) for i in data)
        result = np.fromiter(arr, dtype=dtypes)
    else:
        check = data[0] if len(data) > 0 else []
        if isinstance(check, dict):
            cols = kwargs.get("cols", data[0].keys())  # type: ignore
            dtypes = list(itemgetter(*cols)(columns_dtype))
            arr = [tuple(itemgetter(*cols)(row)) for row in data]
            result = np.array(arr, dtype=dtypes)
        else:
            cols = kwargs.get("cols", kline_columns[2:-1])
            dtypes = list(itemgetter(*cols)(columns_dtype))
            arr = data
            result = np.array(arr, dtype=dtypes)

    return result
