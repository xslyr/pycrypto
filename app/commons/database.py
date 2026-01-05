
import os, psycopg
from typing import Any

from psycopg import sql, Connection
from .utils import BrokerUtils, Singleton, Timing, DatabaseDescription
import numpy as np
import pandas as pd

from psycopg import OperationalError


class Database (metaclass=Singleton):
       

    def __init__(self, **kwargs):

        host = kwargs.get('host', os.getenv('POSTGRES_HOST') )
        port = kwargs.get('port', os.getenv('POSTGRES_PORT') )
        dbname = kwargs.get('dbname', os.getenv('POSTGRES_DB') )
        user = kwargs.get('user', os.getenv('POSTGRES_USER') )
        password = kwargs.get('password', os.getenv('POSTGRES_PASSWORD') )
        connection_string = "dbname={} user={} password={} host={} port={}".format(dbname, user, password, host, port)
        try:
            self._conn = psycopg.connect( connection_string)
            self._conn.autocommit = True
        except OperationalError as e:
            raise Exception('Database connection error. Please verify environment variables and service availability.')


    @property
    def conn(self)->Connection:
        return self._conn


    def rollback(self):
        self.conn.rollback()


    def check_fix_db_integrity(self):
        try:
            cur = self.conn.cursor()
            select_result = cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'AND table_type = 'BASE TABLE'; ").fetchall()
            
            if len(select_result) < DatabaseDescription.qty_tables:
                db_structure = DatabaseDescription.db_structure

                sql_base_create = "CREATE TABLE IF NOT EXISTS {} ({}, {});"
                for table in db_structure.keys():
                    columns = ' , '.join( map( lambda x: ' '.join(x), db_structure[table]['columns'].items() ) )
                    primary_key = db_structure[table]['primary_key']
                    if table == 'klines_tables':
                        for kline in Timing.intervals_available:
                            table_name = f'klines_{kline}'
                            create_command = sql.SQL(sql_base_create).format(sql.Identifier(table_name), sql.SQL(columns), sql.SQL(primary_key))  # type: ignore
                            cur.execute( create_command )

                    else:
                        create_command = sql.SQL( sql_base_create ).format(sql.Identifier(table), sql.SQL(columns), sql.SQL(primary_key))  # type: ignore
                        cur.execute( create_command )
            return True
        
        except Exception as e:
            self.conn.rollback()
            return e
        

    def destroy_all_tables(self, confirmation=False):
        try:
            if confirmation:
                cur = self.conn.cursor()
                select_result = cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'AND table_type = 'BASE TABLE'; ").fetchall()
                
                for line in select_result:
                    drop_command = sql.SQL("DROP TABLE IF EXISTS {} CASCADE;").format( sql.Identifier( line[0]) )
                    cur.execute( drop_command )

                return True
            else:
                return False
            
        except Exception as e:
            return e
        
        
    def clean_kline_table(self, intervals:list[str]):
        try:
            cur = self.conn.cursor()
            for i in intervals:
                table = f"klines_{i}"
                query = sql.SQL("delete from {}").format( sql.Identifier(table) )
                cur.execute(query)
            return True
        except Exception as e:
            self.conn.rollback()
            return e


    def select_klines(self, 
                      ticker:str, 
                      interval:str,
                      from_datetime:Any='', 
                      between_datetimes:tuple[ Any, Any ] = ('',''), **kwargs):
        
        only_columns:list[str] = kwargs.get('only_columns', BrokerUtils.kline_columns[2:-1])
        columns = sql.SQL(', ').join(map(sql.Identifier,only_columns))
        
        if from_datetime == '' and between_datetimes == ('',''):
            raise Exception('Is necessary one of from_datetime or between_datetime param.')
        
        if from_datetime != '' and between_datetimes != ('',''):
            raise Exception('Is necessary ONLY one of from_datetime or between_datetime param.')


        table = f"klines_{interval}"
        if from_datetime != '':
            query = sql.SQL("SELECT {} FROM {} WHERE ticker=%s AND open_time >= %s ").format(columns, sql.Identifier(table))
            params = ( ticker, Timing.convert_any_to_timestamp(from_datetime) )
        
        if all(between_datetimes):
            query = sql.SQL("SELECT {} FROM {} WHERE ticker=%s AND open_time between %s and %s ").format(columns, sql.Identifier(table))
            params = ( ticker, Timing.convert_any_to_timestamp(between_datetimes[0]), Timing.convert_any_to_timestamp(between_datetimes[1]) )

        cur = self.conn.cursor()
        return cur.execute(query, params ).fetchall()
    

    def insert_klines(self, ticker:str, interval:str, data:list):
        try: 
            table = f"klines_{interval}"
            columns = DatabaseDescription.db_structure['klines_tables']['columns'].keys()

            column_names = sql.SQL(', ').join( sql.Identifier(c) for c in columns )
            placeholders = sql.SQL(', ').join( sql.Placeholder() * len(columns) )

            base_sql = "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT (ticker, open_time) DO NOTHING;"
            query = sql.SQL(base_sql).format( sql.Identifier(table), column_names, placeholders) 

            limit = len(data)
            arr1 = np.array( [ticker]*limit ).reshape(-1,1)
            arr2 = np.array(data)[:,:-1]

            data_to_insert = pd.DataFrame(np.hstack((arr1, arr2))).to_records(index=False).tolist()

            cur = self.conn.cursor()
            cur.executemany( query, data_to_insert )

            return True
        
        except Exception as e:
            self.conn.rollback()
            return e
        