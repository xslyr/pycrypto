from operator import itemgetter
from typing import Any

import numpy as np

from pycrypto.commons.utils import BrokerUtils


class ItemRule:
    def __init__(self, *args):
        self.check_and_set_params(*args)

    def check_and_set_params(self, *args):
        data, field, fnc, params = None, None, None, None
        for arg in args:
            match arg:
                case np.ndarray():
                    data = arg
                case str() | list():
                    field = arg
                case _ if callable(arg):
                    fnc = arg
                case dict():
                    params = arg

        self._active_field = field or "close"

        if isinstance(self._active_field, str):
            _dtype = [BrokerUtils.columns_dtype[self._active_field]]
        else:
            _dtype = list(itemgetter(*self._active_field)(BrokerUtils.columns_dtype))

        self._arr = np.asarray(data) if data is not None else np.array([], dtype=_dtype)
        self._fnc = fnc
        self._params = params or {}

    def bind(self, *args):
        self.check_and_set_params(*args)
        return self

    @property
    def active_field(self) -> np.ndarray:
        return self._arr[self._active_field]

    @property
    def last(self):
        return self._arr[-1:]

    def size(self):
        return self._arr.size

    def run(self) -> Any:
        if self._fnc:
            return self._fnc(self._arr, **self._params)
        return None

    def __get_val(self, other):
        return other.active_field[-1].item() if isinstance(other, ItemRule) else other

    def __lt__(self, other):
        return self.active_field[-1].item() < self.__get_val(other)

    def __le__(self, other):
        return self.active_field[-1].item() <= self.__get_val(other)

    def __gt__(self, other):
        return self.active_field[-1].item() > self.__get_val(other)

    def __ge__(self, other):
        return self.active_field[-1].item() >= self.__get_val(other)

    def __eq__(self, other):
        return self.active_field[-1].item() == self.__get_val(other)

    def __ne__(self, other):
        return self.active_field[-1].item() != self.__get_val(other)

    def __bool__(self):
        return bool(self.active_field[-1].item())
