from operator import itemgetter
from typing import Any, Tuple

import numpy as np

from pycrypto.commons.utils import BrokerUtils


class ItemRule:
    """This class is responsible to define one single item of some rule. Each item can be comparable with another one or a simple number.
    An ItemRule object can initialize without data param, in this case receiving just a function that will only point about decision of buying or selling a ticker at any interval.

    Args:
        np.ndarray: array of data that will use to aplly rule conditions
        callable: function of technical analysis to be apply on data to calc criteria of rule.
        str or list: this args indicate the name of field on array, that can be used by function.
            e.g.: SMA(callable param) of 'close' or another field.
        dict: dictionary params will indicate configs to callable functions.

    Properties:
        activy_filed: return a column data of "str/list" param definied on initialization.
        last: return the last line of array data.
        size: return the size of array data.

    Methods:
        bind: procedure to link array on class, allowing to technical analysis function works on it.
        run: return the execution of callable param, with array data considering "str/list" focus field and dict configs. If callable not defined returns None.

    Observation:
        Although array data is not necessary to instantiation, operators between ItemRule objects need data to be executed.
        If a Function not defined, the execution will consider last item of active field,, otherwise will use the last value of function execution.
    """

    def __init__(self, *args):
        self.__check_and_set_params(*args)

    def __check_and_set_params(self, *args, target_operator: int = 0):
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
        self._target_operator = target_operator

    def bind(self, *args):
        self.__check_and_set_params(*args)
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

    def field_result(self):
        if self._fnc:
            run = self.run()
            if isinstance(run, Tuple):
                run[self._target_operator][-1].item()
            return run[-1].item()

        return self.active_field[-1].item()

    def __get_val(self, other):
        return other.field_result() if isinstance(other, ItemRule) else other

    def __lt__(self, other):
        return self.field_result() < self.__get_val(other)

    def __le__(self, other):
        return self.field_result() <= self.__get_val(other)

    def __gt__(self, other):
        return self.field_result() > self.__get_val(other)

    def __ge__(self, other):
        return self.field_result() >= self.__get_val(other)

    def __eq__(self, other):
        return self.field_result() == self.__get_val(other)

    def __ne__(self, other):
        return self.field_result() != self.__get_val(other)

    def __bool__(self):
        return bool(self.field_result())
