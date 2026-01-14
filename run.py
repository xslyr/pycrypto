import json

from pycrypto.commons.utils import DataSources
from pycrypto.trading import convert_data_to_numpy

with open("./tests/mock/BTCUSDT_1h_2025-01-01 00:00:00_dict.json") as f:
    data = json.load(f)

arr = convert_data_to_numpy(data, DataSources.mock)

# by now just for tests
