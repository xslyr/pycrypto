from pycrypto.broker import Broker
from pycrypto.commons.database import Database
from pycrypto.commons.utils import Singleton
from pycrypto.trading.strategy import TradeStrategy


class Orchestrator(metaclass=Singleton):
    """Orchestrator class is responsable for load all necessary data on cache and periodic bulk save of websocket klines on database.

    Args:
        strategy: TradeStrategy object to Orchestrator read all necessary data such ticker, intervals and lengh of technical analysis range needed.

    """

    def __init__(self, strategy: TradeStrategy, **kwargs):
        self.test_mode = kwargs.get("test_mode", False)
        self.database = Database()
        self.broker = Broker(self.test_mode)
        self.strategy = strategy

    def start_cache(self):
        """Method to start data orchestration, that consist in load some data from database and start websocket with necessary intervals.

        # method under construction

        """
