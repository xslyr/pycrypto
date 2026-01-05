
from datetime import datetime, timedelta
from app.commons.database import Database
from app.broker import Broker
from app.commons.utils import Singleton
from app.trading.strategy import TradeStrategy
from .historical_loader import Loader




class Orchestrator(metaclass=Singleton):
   
    def __init__(self, ticker:str, interval_focus:list[str], strategy:TradeStrategy, **kwargs):
        self.test_mode = kwargs('test_mode', False)
        self.database = Database()
        self.broker = Broker(self.test_mode)
        self.interval_focus = kwargs.get('interval_focus',['1m','1h','1d'])
        self.ticker = ticker
        self.strategy = strategy


    def start_cache(self, **kwargs):
        loader = Loader(database=self.database, broker=self.broker)
        if not self.test_mode:

            # conferir o range das estratégias para carregar "from days ago" 

            from_datetime = datetime.now() - timedelta(days=90)
            missing_klines = loader.check_missing_data(self.ticker, self.interval_focus, from_datetime=from_datetime)

            # salvar no banco de dados os klines faltantes
            # carregar no cache os klines closed from range condicionado a analise do range das estratégias (dias / maior tempo em intervals)
            # ligar o websocket para os interval focus

        else:
            from_datetime = kwargs.get('from_datetime', None)
            if not from_datetime: 
                raise Exception('For backtesting trading strategies, is necessary define a start datetime with parameter `from_datetime`.')

            # salvar no banco todos os interval faltantes desde from_datetime            

