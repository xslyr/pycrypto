"""Module responsable to implements Analyst class who will be used to decide when call buy/sell on broker"""

"""
  O analista(analyst) utiliza de uma estratégia de trading(TradeStrategy) ... 
  que define em mercados(Market) as regras(ItemRule) para ações de compra/venda de ativos

  As classes acima instanciam o cenário, ... 
    o orchestrador executa a captura/disponibilização dos dados, salva em cache ou banco e...


"""


class Analyst:
    def __init__(self): ...
