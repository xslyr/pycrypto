import time as ts
import logging
import numpy as np
from typing import Any
from datetime import datetime, time
from app.broker import Broker
from app.commons.database import Database
from app.commons.utils import Timing



# TODO: incluir um tqdm no lugar do logger para ter noção do tempo restante


logger = logging.getLogger("app")

class Loader:

    def __init__(self, test_mode=False, **kwargs):
        self.db = kwargs.get('database', Database())
        self.br = kwargs.get('broker', Broker(test_mode))


    def __check_datetime_params(self, from_datetime, between_datetimes):
        if from_datetime == '' and between_datetimes == ('',''):
                raise Exception('Is necessary one of from_datetime or between_datetime param.')
            
        if from_datetime != '' and between_datetimes != ('',''):
            raise Exception('Is necessary ONLY one of from_datetime or between_datetime param.')


    def __common_datetime_conversions(self, intervals, from_datetime, between_datetimes, verbose):
        tz = Timing.get_timezone()
        if from_datetime != '':
            start = Timing.convert_any_to_datetime(from_datetime)
            end = datetime.combine( datetime.now().date(), time(0,0) ).astimezone(tz)

        if between_datetimes!=('',''):
            start = Timing.convert_any_to_datetime(between_datetimes[0])
            end = Timing.convert_any_to_datetime(between_datetimes[1])
        
        if verbose:
            intv = ', '.join(intervals)
            logger.info(f"Loading Data from Binance to Database\n. Intervals ({intv})\tBetween datetimes ({start} e {end})\n")

        return start, end


    def check_missing_data(self, ticker:str, intervals:list[str], from_datetime:Any='', between_datetimes:tuple[Any,Any]=('',''), **kwargs):
        verbose = kwargs.get('verbose', False)
        self.__check_datetime_params(from_datetime, between_datetimes)
        start, end = self.__common_datetime_conversions(intervals, from_datetime, between_datetimes, verbose)

        remaining_timestamps = {}
        for i in intervals[::-1]:
            timestamp_range = Timing.get_timestamp_range_list(start,end,i)
            if i == '1d':
                timestamps_saved = [int(i[0]/1000)-75600 for i in self.db.select_klines(ticker, i, between_datetimes=(start,end), only_columns=['open_time'])]
            else:
                timestamps_saved = [int(i[0]/1000) for i in self.db.select_klines(ticker, i, between_datetimes=(start,end), only_columns=['open_time'])]

            remaining_timestamps[i] = np.setdiff1d(timestamp_range, timestamps_saved, assume_unique=True).tolist()

        return remaining_timestamps
        
    
    def dump_klines_into_db(self, ticker:str, intervals:list, from_datetime:Any='', between_datetimes:tuple[ Any, Any ] = ('',''), **kwargs):
        try: 

            verbose = kwargs.get('verbose', False)
            self.__check_datetime_params(from_datetime, between_datetimes)

            start, end = self.__common_datetime_conversions(intervals, from_datetime, between_datetimes, verbose)
            
            for i in intervals[::-1]:
                if verbose:
                    logger.info(f".. Dumping interval {i}")

                delta = Timing.delta_intervals[i]
                intervals_between_datetimes = int( ( end - start ) / delta ) 
                full_loops, final_round = divmod( intervals_between_datetimes , 1000 )
                if verbose: 
                    logger.info(f"\tRecords in current interval: {intervals_between_datetimes}")

                date_aux = start
                for _ in range(full_loops):
                    data = self.br.get_klines(ticker, i, date_aux)
                    self.db.insert_klines(ticker,i, data) 
                    if verbose:
                        s = datetime.fromtimestamp(data[0][0]/1000) 
                        e = datetime.fromtimestamp(data[-1][6]/1000)
                        logger.info(f"\t 1000 records saved between {s} and {e}")

                    date_aux += 1000*delta
                    if full_loops > 60: ts.sleep(1)

                data = self.br.get_klines(ticker, i, date_aux)
                self.db.insert_klines(ticker,i, data)
                if verbose:
                        s = datetime.fromtimestamp(data[0][0]/1000)
                        e = datetime.fromtimestamp(data[-1][6]/1000)
                        logger.info(f"\t {final_round} records saved between {s} and {e}")

            return True
    
        except Exception as e:
            self.db.rollback()
            logger.exception('Error on loading binance data into db.')
            
    
        