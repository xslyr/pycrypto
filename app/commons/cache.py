import os, redis, json, logging
from typing import Any
from datetime import datetime
from app.commons.utils import Singleton, BrokerUtils
from redis.exceptions import ConnectionError, ResponseError
from operator import itemgetter

logger = logging.getLogger("app.websocket")

class Cache(metaclass=Singleton):

    _cache: redis.Redis = None  # type: ignore
    key_types = {}

    
    def __init__(self):
        if Cache._cache is None:
            params = {
                'host': os.getenv('REDIS_HOST','localhost'),
                'db': 0,
                'port': int(os.getenv('REDIS_PORT',6379)),
                'socket_keepalive': True,
                'health_check_interval': 30,
                'decode_responses': True
            }
            Cache._maxlen = BrokerUtils.websocket_opened_maxlen
            try:
                Cache._cache = redis.Redis(**params)
                Cache._cache.ping()
            except ConnectionError:
                raise ConnectionError('Redis connection error. Please verify environment variables and service availability.')
            
        
    
    @classmethod
    def save(cls, key:str, value:Any):
        try:
            match value:
                case int():
                    str_value = str(value)
                    cls.key_types[key] = lambda x: int(x)
                case float():
                    str_value = str(value)
                    cls.key_types[key] = lambda x: float(x)
                case dict() | list(): 
                    str_value = json.dumps(value)
                    cls.key_types[key] = lambda x: json.loads(x)
                case datetime():
                    str_value = str(value)
                    cls.key_types[key] = lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
                case str():
                    str_value = value
                    cls.key_types[key] = lambda x: x

            cls._cache.set( key, str_value )
            return True
        
        except Exception as e:
            return e
    

    @classmethod
    def get(cls, key:str):
        try: return cls.key_types[key]( cls._cache.get(key) )
        except: 
            logger.warning(f'Key {key} not exists.')
            return None


    @classmethod
    def search_keys(cls, pattern='*'):
        return cls._cache.keys(pattern)


    @classmethod
    def append_kline(cls, stream:Any, data:dict, closed_klines=True):
        match stream:
            case str(): redis_key = stream
            case tuple()|list(): redis_key = '@kline_'.join(stream) 
            case _: raise Exception('Stream parameter must be a tuple of (ticker,interval) or string like ticker@kline_interval.')
        
        redis_key = redis_key.lower()
        redis_key += ':closed' if closed_klines else ':opened'
        maxlen = cls._maxlen[stream[1]] if isinstance(stream, list|tuple) else cls._maxlen[ stream.split('_')[1] ]

        try:
            opentime = data.get('opentime') or data.get('t')
            cls._cache.xadd( redis_key , {'data':json.dumps(data)}, id=f'{opentime}-*', maxlen=maxlen, approximate=True) 
            return True
        except Exception as e:
            logger.exception(f'Error on append_klines {stream}')
            return None


    @classmethod
    def append_klines(cls, stream:Any, data:list[dict], closed_klines=True):
        pipe = cls._cache.pipeline()
        match stream:
            case str(): redis_key = stream
            case tuple()|list(): redis_key = '@kline_'.join(stream) 
            case _: raise Exception('Stream parameter must be a tuple of (ticker,interval) or string like ticker@kline_interval.')
        
        redis_key = redis_key.lower()
        redis_key += ':closed' if closed_klines else ':opened'
        maxlen = cls._maxlen[stream[1]] if isinstance(stream, list|tuple) else cls._maxlen[ stream.split('_')[1] ]

        try:
            for item in data:
                pipe.xadd( redis_key , {'data':json.dumps(item)}, id=item['opentime'], maxlen=maxlen, approximate=True) 
            pipe.execute()
            return True
        except Exception as e:
            logger.exception(f'Error on append_klines {stream}')
            return None


    @classmethod
    def get_klines(cls, ticker:str, interval:str, min="-", max="+", closed_klines=True, limit=None, only_columns=[] ):

        get_data = lambda d: json.loads(d[1]['data']) 

        try:
            redis_key = f"{ticker.lower()}@kline_{interval}"
            redis_key += ':closed' if closed_klines or interval=='1s' else ':opened'
            _limit = cls._maxlen[interval] if not limit else limit

            raw_data = cls._cache.xrevrange(redis_key, max=max, min=min, count=_limit)
            
            if only_columns == []: return [ get_data(d) for d in raw_data ]
            else: return [ itemgetter( *only_columns, get_data(d) ) for d in raw_data ]
        
        except:
            logger.warning(f'Error on get_klines {redis_key}. Check if this key exists.')
            return None


    @classmethod
    def get_info_stream(cls, ticker:str, interval:str, closed_klines=True):
        try: 
            redis_key = f"{ticker.lower()}@kline_{interval}"
            redis_key += ':closed' if closed_klines else ':opened'
            return cls._cache.xinfo_stream(redis_key)
        
        except ResponseError:  
            logger.warning(f'The key {redis_key} not exists.')
            return None
        
        except Exception as e:
            return e


    @classmethod
    def delete_stream(cls, ticker:str, interval:str, closed_klines=True):
        _stream = f"{ticker.lower()}@kline_{interval}"
        _stream += ':closed' if closed_klines else ':opened'
        try:
            cls._cache.delete(_stream)
            return True
        except Exception as e:
            return e
    

    @classmethod
    def check_key_exists(cls, ticker:str, interval:str, closed_klines=True):
        redis_key = f"{ticker.lower()}@kline_{interval}"
        redis_key += ':closed' if closed_klines else ':opened'
        return bool(cls._cache.exists(redis_key))
    

    @classmethod
    def delete_ids_in_stream(cls, stream:Any, list_ids:list, closed_klines=True):
        match stream:
            case str(): _stream = stream
            case tuple()|list(): _stream = '@kline_'.join(stream) 
            case _: raise Exception('Stream parameter must be a tuple of (ticker,interval) or string like ticker@kline_interval.')
        
        _stream = _stream.lower()
        _stream += ':closed' if closed_klines else ':opened'
        
        pipe = cls._cache.pipeline()
        try:
            pipe.xdel(_stream, list_ids)
            return True
        except:
            logger.exception('Error on deletion stream data.')

    @classmethod
    def delete_data_in_stream(cls, ticker:str, interval:str, nlast:int=0, closed_klines=True):
        pipe = cls._cache.pipeline()

        _stream = f"{ticker.lower()}@kline_{interval}"
        _stream += ':closed' if closed_klines else ':opened'
        try:
            pipe.xtrim(_stream, maxlen=nlast, approximate=True)
            return True
        except:
            logger.exception('Error on deletion stream data.')

    
    @classmethod
    def flushdb(cls):
        cls._cache.flushdb()