"""Module responsable to implements Parent child of rules criteria for strategies and Aggregate technical analysis functions."""

from enum import Enum
from typing import Any, Tuple, overload

import numpy as np
import talib

from pycrypto.trading import ItemRule

# https://github.com/TA-Lib/ta-lib-python/
# https://github.com/TA-Lib/ta-lib-python/tree/master/docs/func_groups


class MAType(Enum):
    SMA = 0
    EMA = 1
    WMA = 2
    DEMA = 3
    TEMA = 4
    TRIMA = 5
    KAMA = 6
    MAMA = 7
    T3 = 8


SMATarget = Enum(
    "SMATarget",
    [
        "open",
        "close",
        "high",
        "low",
        "base_asset_volume",
        "quote_asset_volume",
        "number_of_trades",
        "taker_buy_base_asset_volume",
        "taker_buy_quote_asset_volume",
    ],
)


class Overlap:
    """Wrapper class to agregate some technical analysis functions"""

    @overload
    @staticmethod
    def sma(data: ItemRule, length: int = 14): ...

    @overload
    @staticmethod
    def sma(data: np.ndarray, length: int = 14, target: str = "close"): ...

    @staticmethod
    def sma(data: Any, **kwargs) -> Any:  # type: ignore
        length = kwargs.get("length", 14)
        if isinstance(data, ItemRule):
            x = talib.SMA(np.array(data), length)[-1]
            return x

        target = kwargs.get("target", "close")
        return talib.SMA(data[target], length)

    @staticmethod
    def bollinger(
        data: np.array,
        lenght: int = 5,
        devup: float = 2,
        devdown: float = 2,
        matype: MAType = MAType.SMA,
    ) -> Tuple[ItemRule, ItemRule]:
        """Bollinger Bands (BBANDS)
        Avalia a volatilidade e define níveis relativos de preços altos e baixos.

        Returns:
            UPPER:    Banda superior (Média + n desvios padrões).
            MIDDLE:   Média móvel simples do período.
            LOWER:    Banda inferior (Média - n desvios padrões).

        Interpretações:
            Quando as bandas se estreitam, indica baixa volatilidade e um provável rompimento eminente.
            Preço saindo da banda e retornando sugere reversão à média.

        Uso:
            Usar para setups de volatilidade e reversão à média.

        """
        r = talib.BBANDS(data["close", lenght, devup, devdown, matype])
        return ItemRule(r[0]), ItemRule(r[1])


class Momentum:
    """Wrapper class to agregate some technical analysis functions"""

    @staticmethod
    def rsi(data: np.array, lenght: int = 14) -> ItemRule:
        """RSI (Relative Strength Index)
        Mede a velocidade e a mudança dos movimentos de preços para identificar condições de sobrecompra ou sobrevenda.

        Retorno:
            RSI:   Valor entre 0 e 100.

        Interpretações:
            Maior que 70:     Sobrecompra. Potencial de correção ou reversão para baixa.
            Menor que 30:     Sobrevenda. Potencial de repique ou reversão para alta.

        Uso:
            Usar para identificar exaustão de movimento em mercados laterais ou para confirmar força de tendência.

        """
        return ItemRule(talib.RSI(data["close"], lenght))

    @staticmethod
    def macd(data: np.array, fast: int = 12, slow: int = 26, signal: int = 9) -> Tuple[ItemRule, ItemRule]:
        """MACD (Moving Average Convergence Divergence)
        O MACD é um oscilador de tendência que mede o momentum através da diferença entre duas médias móveis exponenciais (EMA).

        Retorno:
            MACD:         Indica a direção e força da tendência
            MACDSIGNAL:   Linha de "sinal". Atua como gatilho
            MACDHIST:     Histograma(MACD-SIGNAL). Representa a aceleração do movimento.

        Interpretações:
            Se MACD cruza acima de SIGNAL, sinal de compra (momentum positivo).
            Se preço subindo enquando HIST cai, sugere exaustão da tendência.

        Uso:
            Usar em mercados com tendência definida, para identificar reversões ou força de movimento.

        """
        r = talib.MACD(data["close"], fast, slow, signal)
        return ItemRule(r[0]), ItemRule(r[1])

    @staticmethod
    def stoch(
        data: np.array,
        fastk: int = 5,
        slowk: int = 3,
        slowk_type: MAType = MAType.SMA,
        slowd: int = 3,
        slowd_type=MAType.SMA,
    ) -> Tuple[ItemRule, ItemRule]:
        """Stochastic (STOCH)
        Compara o preço de fechamento atual com a amplitude de preços (mínima e máxima) em um período.

        Retorno:
            SLOWK:    A linha principal do oscilador (%K).
            SLOWD:    A média móvel da linha %K (%D).

        Interpretações:
            Assim como o RSI, foca em 80 (sobrecompra) e 20 (sobrevenda).
            SLOWK cruzando acima de SLOWD abaixo do nível 20 é um sinal clássico de entrada.

        Uso:
            Usar e em mercados laterais ou para refinar o timing de entrada em correções de tendência.

        """
        r = talib.STOCH(
            data["high"],
            data["low"],
            data["close"],
            fastk,
            slowk,
            slowk_type,
            slowd,
            slowd_type,
        )
        return ItemRule(r[0]), ItemRule(r[1])

    @staticmethod
    def adx(data: np.array, lenght: int = 14) -> ItemRule:
        """ADX (Average Directional Movement Index)
        O ADX mede a força de uma tendência, independentemente da direção (se é alta ou baixa).

        Retorno:
            ADX:  Valor maior ou igual a 0.

        Interpretações:
            Abaixo de 20-25: Mercado sem tendência (lateral/consolidado). Indicadores de momentum (RSI, Stoch) funcionam melhor aqui.
            Acima de 25: Tendência forte iniciando. Médias móveis e MACD tornam-se mais confiáveis.
            Acima de 40-50: Tendência extrema; sugere que o movimento pode estar próximo de uma exaustão.

        Uso:
            Usar antes de executar qualquer estratégia, para decidir se deve usar algoritmos de seguimento de tendência ou de reversão à média.

        """
        return ItemRule(talib.ADX(data["high"], data["low"], data["close"], lenght))

    @staticmethod
    def mfi(data: np.array, lenght: int = 14) -> ItemRule:
        """MFI (Money Flow Index)
        É essencialmente um "RSI ponderado pelo volume".

        Retorno:
            MFI:    Valor entre 0 e 100.

        Interpretações:
            > 80: Sobrecompra com volume forte.
            < 20: Sobrevenda com volume forte.

        Uso:
            O MFI é menos sensível a anomalias de preço que ocorrem com pouco volume, sendo mais robusto para evitar ruídos de baixa liquidez.

        """
        return ItemRule(
            talib.MFI(
                data["high"],
                data["low"],
                data["close"],
                data["base_asset_volume"],
                lenght,
            )
        )


class Volume:
    """Wrapper class to agregate some technical analysis functions"""

    @staticmethod
    def obv(data: np.array) -> ItemRule:
        """OBV (On-Balance Volume)
        Um dos indicadores de volume mais utilizados para detectar o fluxo de "smart money".

        Retorno:
            OBV:    Um total cumulativo de volume. Se o fechamento atual > fechamento anterior, o volume é somado; se menor, é subtraído.

        Interpretações:
            Confirmação: Se o preço sobe e o OBV sobe, a tendência é saudável.
            Divergência: Se o preço atinge nova máxima, mas o OBV não, há falta de pressão compradora (provável reversão).

        Uso:
            Usar para validar rompimentos (breakouts). Um rompimento de preço sem aumento no OBV costuma ser um bull/bear trap.

        """
        return ItemRule(talib.OBV(data["close"], data["base_asset_volume"]))

    @staticmethod
    def chaikin_ad(data: np.array) -> ItemRule:
        """Chaikin A/D Line (Accumulation/Distribution Line)
        É a base de todos os indicadores de Chaikin. É uma linha cumulativa que soma ou subtrai o volume de cada período com base no fechamento em relação ao range (high-low).

        Retorno:
            AD:     Fluxo Interno. Retorna uma série de valores reais (floats) que representam a soma acumulada do volume ponderado pelo fechamento de cada candle.

        Interpretações:
            Avalia se o mercado está acumulando (comprando) ou distribuindo (vendendo) ativos.
            Se o preço fecha perto da máxima do período com volume alto, o valor do indicador sobe drasticamente.

        Uso:
            Usar para identificar acumulação institucional antes de um movimento de alta explosivo.

        """
        return ItemRule(talib.AD(data["high"], data["low"], data["close"], data["base_asset_volume"]))

    @staticmethod
    def chaikin_osc(data: np.array, fast: int = 3, slow: int = 10) -> ItemRule:
        """Chaikin Oscillator
        Este é o "MACD do indicador A/D". Ele mede a aceleração do fluxo de dinheiro.

        Retorno:
            ADOSC:  Valor entre -1 e 1 indicando tendência de acumulação ou diluição.

        Interpretações:
            Ele antecipa rompimentos. Se o ADOSC cruza o zero para cima, a velocidade da acumulação está aumentando bruscamente.

        Uso:
            Serve para identificar mudanças rápidas no momentum do volume antes mesmo que o preço mude de direção.

        """
        return ItemRule(
            talib.ADOSC(
                data["high"],
                data["low"],
                data["close"],
                data["base_asset_volume"],
                fast,
                slow,
            )
        )


class Cycle:
    """Este grupo utiliza a Transformada de Hilbert para decompor a série temporal de preços em componentes de ciclo.
    Diferente de médias móveis, eles tentam isolar a periodicidade do mercado.

    Usar para evitar o lag de indicadores tradicionais. Se HT_TRENDMODE for 0, você deve priorizar osciladores;
    se for 1, deve priorizar rastreadores de tendência.
    """

    @staticmethod
    def sine(data: np.array) -> Tuple[ItemRule, ItemRule]:
        """Retorna duas colunas (sine, leadsine). É um oscilador que antecipa cruzamentos antes que o preço confirme a reversão."""
        r = talib.HT_SINE(data["close"])
        return ItemRule(r[0]), ItemRule(r[1])

    @staticmethod
    def trendmode(data: np.array) -> Tuple[ItemRule, ItemRule]:
        """Retorna 0 ou 1. Indica se o mercado está em "Modo Ciclo" (lateral) ou "Modo Tendência"."""
        r = talib.HT_TRENDMODE(data["close"])
        return ItemRule(r[0]), ItemRule(r[1])


class Patterns:
    """Essa classe oferece funções que identifica tendências analisando o formato dos candles segundo padrões previamente conhecidos.

    Na TA-Lib, todas as funções de padrão retornam valores inteiros:
        +100: Padrão de Reversão/Continuação de Alta (Bullish).
        -100: Padrão de Reversão/Continuação de Baixa (Bearish).
        0: Padrão não identificado.

    PADRÕES DE REVERSÃO:
        - engulfing
        - hammer
        - morning_star
        - shooting_star
        - evening_star

    PADRÕES DE CONTINUAÇÃO:
        - line_strike_3
        - rise_fall_3methods
    """

    @staticmethod
    def engulfing(data: np.array) -> ItemRule:
        """(Topos e Fundos) - Engolfo:    Um dos mais confiáveis. O corpo da vela atual "engole" o corpo da anterior."""
        return ItemRule(talib.CDLENGULFING(data["open"], data["high"], data["low"], data["close"]))

    @staticmethod
    def hammer(data: np.array) -> ItemRule:
        """(Fundos) - Martelo: Identificam rejeição de preço em fundos."""
        return ItemRule(talib.CDLHAMMER(data["open"], data["high"], data["low"], data["close"]))

    @staticmethod
    def morning_star(data: np.array, penetration: float = 0) -> ItemRule:
        """(Fundos) - Estrela da Manhã: Padrões de três velas que sinalizam o fim de uma tendência e início de outra."""
        return ItemRule(talib.CDLMORNINGSTAR(data["open"], data["high"], data["low"], data["close"], penetration))

    @staticmethod
    def shooting_star(data: np.array) -> ItemRule:
        """(Topos) - Estrela Cadente: Identificam rejeição de preço em topos."""
        return ItemRule(talib.CDLSHOOTINGSTAR(data["open"], data["high"], data["low"], data["close"]))

    @staticmethod
    def evening_star(data: np.array, penetration: float = 0) -> ItemRule:
        """(Topos) - Estrela da Noite: Padrões de três velas que sinalizam o fim de uma tendência e início de outra."""
        return ItemRule(talib.CDLEVENINGSTAR(data["open"], data["high"], data["low"], data["close"], penetration))

    # ----- PADRÕES DE CONTINUAÇÃO -----

    @staticmethod
    def line_strike_3(data: np.array) -> ItemRule:
        """Identifica uma pausa (3 velas) seguida por uma continuação forte na direção original."""
        return ItemRule(talib.CDL3LINESTRIKE(data["open"], data["high"], data["low"], data["close"]))

    @staticmethod
    def rise_fall_3methods(data: np.array) -> ItemRule:
        """Um padrão de 5 velas que confirma a força da tendência vigente após uma pequena consolidação."""
        return ItemRule(talib.CDLRISEFALL3METHODS(data["open"], data["high"], data["low"], data["close"]))
