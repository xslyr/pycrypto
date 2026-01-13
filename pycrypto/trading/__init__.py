"""Package trading aggregate objects who will define and create strategy rules and analyst who will be used to decide and call buy/sell on broker"""

from operator import itemgetter

import numpy as np

from pycrypto.commons.utils import BrokerUtils, DataSources
from pycrypto.trading.rules import ItemRule


def convert_data_to_numpy(data: list, origin: DataSources, **kwargs) -> np.array:
    """Method to prepare and convert crude data to numpy array"""
    result = None
    match origin:
        case DataSources.websocket:
            cols = kwargs.get("cols", BrokerUtils.kline_columns[2:-1])
            dtypes = list(itemgetter(*cols)(BrokerUtils.columns_dtype))
            col_ids = itemgetter(*cols)(BrokerUtils.ws_columns_names)
            arr = (itemgetter(*col_ids)(i) for i in data)
            result = np.fromiter(arr, dtype=dtypes)

        case DataSources.database:
            cols = kwargs.get("cols", BrokerUtils.kline_columns[2:-1])
            dtypes = list(itemgetter(*cols)(BrokerUtils.columns_dtype))
            arr = data
            result = np.array(arr, dtype=dtypes)

        case DataSources.mock:
            cols = kwargs.get("cols", data[0].keys())
            dtypes = list(itemgetter(*cols)(BrokerUtils.columns_dtype))
            arr = [tuple(itemgetter(*cols)(row)) for row in data]
            result = np.array(arr, dtype=dtypes)

    return result
