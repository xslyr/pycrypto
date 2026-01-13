"""Module responsable to implements TradeStrategy class who necessary to define 1 single criteria"""

from pycrypto.trading import ItemRule


class TradeStrategy:
    """Class to init some single Rule Criteria"""

    # TODO: implements the way to init some criteria

    def __init__(self):
        self.rule = [{"1m": ItemRule(), "1h": None, "1d": None}]
