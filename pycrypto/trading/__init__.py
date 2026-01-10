"""Package trading aggregate objects who will define and create strategy rules and analyst who will be used to decide and call buy/sell on broker"""

from operator import itemgetter

import numpy as np

from pycrypto.commons.utils import BrokerUtils, DataSources


class ItemRule(np.ndarray):
    """Class is used as parent of Strategies combinations and implements numpy operations between our prepared numpy array"""

    def __new__(cls, arr):
        return np.asarray(arr).view(cls)

    def __bool__(self):
        return bool(self[-1])

    def __get_value(self, other):
        return other[-1] if isinstance(other, np.ndarray) else other

    def __lt__(self, other):
        return self[-1] < self.__get_value(other)

    def __le__(self, other):
        return self[-1] <= self.__get_value(other)

    def __gt__(self, other):
        return self[-1] > self.__get_value(other)

    def __ge__(self, other):
        return self[-1] >= self.__get_value(other)

    def __eq__(self, other):
        return self[-1] == self.__get_value(other)

    def __ne__(self, other):
        return self[-1] != self.__get_value(other)


# TODO: Create test to this test
def convert_data_to_numpy(data: list, origin: DataSources, **kwargs) -> np.array:
    """Method to prepare and convert crude data to numpy array"""

    result = None

    match origin:
        case DataSources.websocket:
            cols = kwargs.get("cols", BrokerUtils.kline_columns[2:-1])
            dtypes = list(itemgetter(*cols)(BrokerUtils.ws_columns_dtype))
            col_ids = itemgetter(*cols)(BrokerUtils.ws_columns_names)
            arr = (itemgetter(*col_ids)(i) for i in data)
            result = np.fromiter(arr, dtype=dtypes)

        case DataSources.database:
            cols = kwargs.get("cols", BrokerUtils.kline_columns[2:-1])
            dtypes = list(itemgetter(*cols)(BrokerUtils.ws_columns_dtype))
            arr = data
            result = np.array(arr, dtype=dtypes)

        case DataSources.mock:
            cols = data[0].keys()
            dtypes = list(itemgetter(*cols)(BrokerUtils.ws_columns_dtype))
            arr = [tuple(row.values()) for row in data]
            result = np.array(arr, dtype=dtypes)

    return result
