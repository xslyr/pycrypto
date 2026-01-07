# from app.commons import Database
from dotenv import load_dotenv

load_dotenv()

# Database().check_fix_db_integrity()
# Loader().dump_klines_into_db('BTCUSDT',['1m','1h','1d'], from_datetime='2025-01-01 00:00:00', verbose=True)

# Loader().check_missing_data('BTCUSDT',['1m','1h','1d'], from_datetime='2025-01-01 00:00:00')

"""

  intervalos alvos p/ análise: 1d, 1h, 15m, 3m, ...( 1m/tempo real apenas após validação inicial )

  requisitos:
    - valor inicial de carteira
    - 'tamanho da mão' a ser negociado em cada intervalo
    - intervalos que haverá negociacao
    - parametros para o sma, adx e rsi serão usados em cada intervalo
    - hierarquia de condicoes para compra e venda
    - teste de performance simulando uma ocorrência real

    
  entidades / caracteristicas ou metodos principais:
    - broker                : consultar saldo, comprar, vender
    - data orchestrator     : sma, rsi, adx
    - trader analyst        : regras, stack
    

"""
