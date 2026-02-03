"""Module responsable to implements TradeStrategy class who necessary to define 1 single criteria"""

from pycrypto.commons.utils import KlinesIntervals

type Conditions = dict[KlinesIntervals, list[bool]]


class Market:
    buy: Conditions = {}
    sell: Conditions = {}


class TradeStrategy:
    """Class to define strategies with all rules criteria and serve it to orchestration"""

    up_market: Market = Market()
    down_market: Market = Market()
    side_market: Market = Market()
