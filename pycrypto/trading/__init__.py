"""Package trading aggregate objects who will define and create strategy rules and analyst who will be used to decide and call buy/sell on broker"""

from operator import itemgetter
from typing import Any, Callable, Optional, Self, cast, overload

import numpy as np

from pycrypto.commons.utils import BrokerUtils, DataSources


class ItemRule(np.ndarray):
    """Class is used as parent of Strategies combinations and implements numpy operations between our prepared numpy array"""

    _active_field: str
    _fnc: Optional[Callable]
    _params: dict[str, Any]

    def __new__(cls, arr, field=None, fnc=None, params=None):
        obj = np.asarray(arr).view(cls)
        obj._active_field = field or "close"  # Default field
        obj._fnc = fnc
        obj._params = params or {}
        return obj

    def __array_finalize__(self, obj):
        if obj is None:
            return
        self._active_field = getattr(obj, "_active_field", "close")

    def run_fnc(self) -> Any:
        if self._fnc:
            return self._fnc(self[self._active_field], **self._params)
        return None

    @overload
    def __getitem__(self, item: str) -> Self: ...

    @overload
    def __getitem__(self, item: int) -> np.void: ...

    def __getitem__(self, item: Any) -> Any:
        res = super().__getitem__(item)
        if isinstance(item, str):
            res = res.view(type(self))
            res._active_field = item
        return cast(ItemRule, res)

    def _get_cmp_data(self):
        if self.dtype.names is not None:
            return np.ndarray.__getitem__(self, self._active_field)[-1]
        return self

    @property
    def active_field(self) -> Self:
        if self.dtype.names is not None:
            res = self[self._active_field]
            return res.view(type(self)) if not isinstance(res, ItemRule) else res
        return self

    @property
    def last(self):
        return self[-1:]

    def __bool__(self):
        return bool(self.active_field.last.item())

    def __get_value(self, other):
        return other.active_field[-1] if isinstance(other, ItemRule) else other

    def __lt__(self, other):
        return np.asarray(self.active_field)[-1] < self.__get_value(other)

    def __le__(self, other):
        return np.asarray(self.active_field)[-1] <= self.__get_value(other)

    def __gt__(self, other):
        return np.asarray(self.active_field)[-1] > self.__get_value(other)

    def __ge__(self, other):
        return np.asarray(self.active_field)[-1] >= self.__get_value(other)

    def __eq__(self, other):
        return np.asarray(self.active_field)[-1] == self.__get_value(other)

    def __ne__(self, other):
        return np.asarray(self.active_field)[-1] != self.__get_value(other)


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
            cols = kwargs.get("cols", data[0].keys())
            dtypes = list(itemgetter(*cols)(BrokerUtils.ws_columns_dtype))
            arr = [tuple(itemgetter(*cols)(row)) for row in data]
            result = np.array(arr, dtype=dtypes)

    return result
