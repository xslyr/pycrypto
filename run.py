import json

from pycrypto.commons.utils import DataSources
from pycrypto.trading import ItemRule, convert_data_to_numpy
from pycrypto.trading.technical_analysis import Overlap

with open("./tests/mock/BTCUSDT_1h_2025-01-01 00:00:00_dict.json") as f:
    data = json.load(f)

arr = convert_data_to_numpy(data, DataSources.mock)

ir = ItemRule(arr, field="close", fnc=Overlap.sma, params={"length": 5})
ir_func = ir.run_fnc()

sma_arr = Overlap.sma(arr, length=5)

print(f" ir: {ir_func} \t sma_arr: {sma_arr[-1]} ")
