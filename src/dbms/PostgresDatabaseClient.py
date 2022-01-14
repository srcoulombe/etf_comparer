# standard library dependencies
from typing import List, Any, Iterable, Union

# external dependencies
import psycopg

# local dependencies
from SQLDatabaseClient import SQLDatabaseClient

class PostgresDatabaseClient(SQLDatabaseClient):

    def __init__(   self, 
                    connection_str: str = "dbname=etf_analyzer user=samy"):
        super().__init__(dbms = "postgres")
        self.__connection_str = connection_str 

    @property
    def holdings_table_creation_query(self) -> str:
        """Postgres query to create the `holdings_table` table"""
        return '''CREATE TABLE IF NOT EXISTS holdings_table(
            Holding_ID serial PRIMARY KEY,
            Holding varchar(255) unique
        );
        '''

    @property
    def etf_ticker_table_creation_query(self) -> str:
        """Postgres query to create the `etf_ticker_table` table"""
        return '''CREATE TABLE IF NOT EXISTS etf_ticker_table(
            ETF_ticker_ID serial PRIMARY KEY,
            ETF_ticker varchar(255) unique
        );
        '''

    @property
    def etf_table_creation_query(self) -> str:
        """Postgres query to create the `etf_holdings_table` table"""
        return '''CREATE TABLE IF NOT EXISTS etf_holdings_table(
            Row_ID serial PRIMARY KEY,
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
        # https://www.psycopg.org/psycopg3/docs/basic/usage.html#connection-context
        with psycopg.connect(self.__connection_str) as conn:
            cur = conn.execute(query, *args)
        try:
            return cur.fetchall()
        except Exception:
            return None   
    
    def execute_query_over_many_arguments(  self, 
                                            query: str, 
                                            args: Iterable[Any],
                                            get_results: bool = False) -> Union[None,List[Any]]:
        # https://www.psycopg.org/psycopg3/docs/basic/usage.html#connection-context
        # https://stackoverflow.com/questions/8134602/psycopg2-insert-multiple-rows-with-one-query
        results: List[Any] = []
        for arg in args:
            with psycopg.connect(self.__connection_str) as conn:
                cur = conn.execute(query, arg)
                if get_results:
                    results.append(cur.fetchall())
        return results if get_results else None
            
