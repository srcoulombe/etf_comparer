# standard library dependencies
import sqlite3
from typing import List, Any, Iterable, Union

# local dependencies
from .SQLDatabaseClient import SQLDatabaseClient

class SQLite3DatabaseClient(SQLDatabaseClient):

    def __init__(   self, 
                    connection_str: str = "data/etf.sqlite"):
        super().__init__(dbms = "sqlite3")
        self.__connection_str = connection_str 
        self.setup()
    
    @property
    def holdings_table_creation_query(self) -> str:
        """SQLite3 query to create the `holdings_table` table"""
        return '''CREATE TABLE IF NOT EXISTS holdings_table(
            Holding_ID integer PRIMARY KEY,
            Holding varchar(255) unique
        );
        '''

    @property
    def etf_ticker_table_creation_query(self) -> str:
        """SQLite3 query to create the `etf_ticker_table` table"""
        return '''CREATE TABLE IF NOT EXISTS etf_ticker_table(
            ETF_ticker_ID integer PRIMARY KEY,
            ETF_ticker varchar(255) unique
        );
        '''

    @property
    def etf_table_creation_query(self) -> str:
        """SQLite3 query to create the `etf_holdings_table` table"""
        return '''CREATE TABLE IF NOT EXISTS etf_holdings_table(
            Row_ID integer PRIMARY KEY,
            Date DATE,
            ETF_ticker_ID integer,
            Holding_ID integer,
            Holding_Weight real,
        FOREIGN KEY (ETF_ticker_ID) REFERENCES etf_ticker_table (ETF_ticker_ID),
        FOREIGN KEY (Holding_ID) REFERENCES holdings_table (Holding_ID)
        );
        '''
    
    def execute_query(  self, 
                        query: str, 
                        *args) -> Union[None,List[Any]]:
        with sqlite3.connect(self.__connection_str, detect_types=sqlite3.PARSE_DECLTYPES) as conn:
            cur = conn.execute(query, *args)
        return cur.fetchall()
    
    def execute_query_over_many_arguments(  self, 
                                            query: str, 
                                            args: Iterable[Any]) -> Union[None,List[Any]]:
        with sqlite3.connect(self.__connection_str, detect_types=sqlite3.PARSE_DECLTYPES) as conn:
            cur = conn.executemany(
                query, 
                args
            )
        return cur.fetchall()

    

        