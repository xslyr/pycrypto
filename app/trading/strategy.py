import app.technical_analysis as ta

class TradeStrategy:
    pass



teste0 = {
    'interval_focus':['1m','1h','1d'],
    'buy_conditions': { 
        '1m': [ (ta.Momentum.rsi,5),"<",50,'or',(ta.Overlap.sma,3),'>',(ta.Overlap.sma,7) ],
        '1h': [ (ta.Momentum.mfi,3),'>',60, 'and', (ta.Volume.chaikin_osc),'>',0 ],
        '1d':[ (ta.Momentum.mfi,3) > 50 ]
    },
    'sell_conditions': { 
        '1m': [ (ta.Momentum.rsi,5),">",50,'or',(ta.Overlap.sma,3),'<',(ta.Overlap.sma,7) ],
        '1h': [ (ta.Momentum.mfi,3),'<',40, 'and', (ta.Volume.chaikin_osc),'<',0 ],
        '1d':[ (ta.Momentum.mfi,3) < 50 ]
    },
}



