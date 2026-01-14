"""Module responsable to implements TradeStrategy class who necessary to define 1 single criteria"""

from typing import Self


class Market(dict):
    def __init__(self, data):
        super().__init__(data)
        for key, value in data.items():
            if any(char in key for char in [" ", "."]):
                raise Exception("Space and dots are not allowed for keys on Market objects.")

            if isinstance(value, dict):
                self.__dict__[key] = Market(value)
            else:
                self.__dict__[key] = value

    @property
    def has_items(self) -> bool:
        return len(self.keys()) > 0

    @property
    def items(self) -> list:
        return list(self.keys())

    def __getattr__(self, item) -> Self | None:
        try:
            return self.__dict__[f"{item[1:]}"] if item[0] == "_" else self.__dict__[item]
        except KeyError:
            return None


class TradeStrategy:
    """Class to define strategies with all rules criteria and serve it to orchestration"""

    __on_up_market: Market
    __on_down_market: Market
    __on_side_market: Market

    def __init__(
        self,
        up_market: Market,
        down_market: Market,
        side_market: Market = None,
    ):
        self.__on_up_market = up_market
        self.__on_down_market = down_market
        self.__on_side_market = side_market

    @property
    def on_up_market(self) -> Market:
        return self.__on_up_market

    @property
    def on_down_market(self) -> Market:
        return self.__on_down_market

    @property
    def on_side_market(self) -> Market:
        return self.__on_side_market
