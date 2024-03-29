# standard library dependencies
import sqlite3
import logging 
logger = logging.getLogger(f"mainLogger.SQLite3DatabaseClient")
import datetime
from datetime import date
from functools import lru_cache
from typing import List, Any, Iterable, Union, Tuple, Mapping

# local dependencies
from .SQLDatabaseClient import SQLDatabaseClient
from ..scraping import scrape_etf_holdings

class SQLite3DatabaseClient(SQLDatabaseClient):

    def __init__(   self, 
                    connection_str: str = "data/etf.sqlite"):
        super().__init__(dbms = "sqlite3")
        self.__connection_str = connection_str 
        self.__placeholder = '?'
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

    def insert_etf_holding_data(self,
                                etf_ticker: str,
                                etf_holdings: Mapping[str, Mapping[str, float]]) -> Union[None,List[Tuple[datetime.date, str, str, float]]]:
        """Method handling everything required to handle the data
        scraped for the specified ETF and insert it into the database.

        Parameters
        ----------
        etf_ticker : str
            ETF of interest.
        etf_holdings: Mapping[str, Mapping[str, float]]
            The holdings data that was scraped for the ETF.
        
        Returns
        -------
        Union[None,List[Tuple[datetime.date, str, str, float]]]
            Either None (if no scraping data was provided) or the
            list of holdings records for the ETF.
        """
        try:
            assert len(etf_holdings) > 0
        except AssertionError as ae:
            logger.warning("SQLite3DatabaseClient.insert_data was given an empty mapping; No updates to be made.")
            return None
        
        etf_ticker = etf_ticker.upper()
        holding_tickers = [ (holding, ) for holding in etf_holdings.keys() ]
        
        # NOTE: this insertion operation doesn't require cleanup if the method
        #       raises an exception afterwards; having more holding tickers in 
        #       `holding_table` is actually advantageous
        try:
            logger.info("Inserting holding tickers.")
            self.execute_query_over_many_arguments(
                f"INSERT INTO holdings_table (Holding) VALUES ({self.__placeholder} );",
                holding_tickers
            )
            logger.info("Inserted holding tickers.")
        except Exception as already_exists:
            logger.warning(already_exists)
            
        # OK we can't use executemany for `select` queries like
        # holding_tickers_and_id = self.execute_query_over_many_arguments(
        #     "select * from holdings_table where Holding = (%s)",
        #     holding_tickers
        # )
        # so we'll need to loop over them
        for (holding_ticker, ) in holding_tickers:
            holding_id: int = self.get_holding_id_for_ticker(holding_ticker)
            # keeping `holding_ticker` and `holding_ticker_id` together is
            # useful for future reference
            etf_holdings[holding_ticker]['holding_ticker_id'] = holding_id

        try:
            logger.info("Inserting etf ticker.")
            self.execute_query(
                #"insert or ignore into etf_ticker_table (ETF_ticker) values (%s);",
                f"INSERT INTO etf_ticker_table (ETF_ticker) VALUES ({self.__placeholder});",
                (etf_ticker,)
            )
            logger.info(f"Successfully inserted {etf_ticker} into etf_ticker_table")
        except Exception as already_exists:
            # NOTE: we can't use ON CONFLICT IGNORE with SQLite3
            #       so we just log the exception
            logger.warning(already_exists)

        etf_ticker_id = self.get_etf_id_for_ticker(etf_ticker)

        holdings = [
            (self.today, etf_ticker_id, holding_dict['holding_ticker_id'], holding_dict['weight'])
            for holding_dict in etf_holdings.values()
        ]
        try:
            logger.info("Inserting into etf_holdings_table.")
            self.execute_query_over_many_arguments(
                f"""INSERT INTO etf_holdings_table 
                (Date, ETF_ticker_ID, Holding_ID, Holding_Weight)
                VALUES ({self.__placeholder}, {self.__placeholder}, {self.__placeholder}, {self.__placeholder});
                """,
                holdings
            )
            logger.info("Inserted into etf_holdings_table.")
        except Exception as error_in_insertion:
            self.execute_query(
                f"DELETE FROM etf_ticker_table WHERE ETF_ticker = %{self.__placeholder};",
                (etf_ticker,)
            )
            logger.warning(f"Caught error in inserting {etf_ticker} in `etf_holdings_table`: {error_in_insertion}")

        finally:
            return holdings


    @lru_cache(maxsize = None)
    def get_holdings_and_weights_for_etf(   self, 
                                            etf_ticker: str,
                                            date_: date = None) -> List[Tuple[datetime.date, str, str, float]]:  
        """LRU-cached method to fetch holding and weights data for the 
        ETF specified by the `etf_name` ticker from the TinyDB database. 
        If no data for said ETF is present, this function will attempt to scrape
        (see `..scraping.scrape_etf_holdings`) and insert it into the TinyDB database.

        Parameters
        ----------
        etf_name : str
            Ticker for the ETF of interest.
        date_ : date, optional
            `datetime.date` object representing the date of interest.
            Defaults None, which gets replaced by today's date.
        
        Returns
        -------
        Mapping[str, Mapping]
            Dictionary mapping the ETF's ticker (`etf_name`) to a dictionary
            mapping the ETF's holdings (strings) to metadata (e.g. the holding's weight 
            w.r.t. the ETF).
        """  

        query = f"""SELECT major.Date, minor.ETF_ticker, other.Holding, major.Holding_Weight 
        FROM (
            select etf_holdings_table.* from etf_holdings_table where ETF_ticker_ID = {self.__placeholder} 
            and Date = {self.__placeholder} 
        ) as major 
        INNER JOIN etf_ticker_table as minor on major.ETF_ticker_ID = minor.ETF_ticker_ID 
        LEFT JOIN holdings_table as other on major.Holding_ID = other.Holding_ID;
        """
        if date_ is None:
            date_ = self.today
        if date_ > self.today:
            raise ValueError(f"Unable to fetch data from {date_}; Functionality to look into the future is not supported yet.")
        etf_ticker = etf_ticker.upper()
        
        try:
            etf_id = self.get_etf_id_for_ticker(etf_ticker)
            holdings: List[Tuple[datetime.date, str, str, float]] = self.execute_query(
                query, 
                (etf_id, date_)
            )
            assert len(holdings) > 0

        except Exception as no_current_data_for_etf:
            # first check that the date is today;
            # will not change data for previous days since 
            # that data is not available anymore
            if date_ != self.today:
                raise ValueError(f"No data is available for {etf_ticker} on {date_}")

            # this could happen if the process of inserting data
            # on the ETF's holdings was interrupted
            if isinstance(no_current_data_for_etf, AssertionError):
                logger.warning(f"Holdings for ETF '{etf_ticker}' were incomplete; updating...")
                
                # do some database cleanup
                # clean up etf_holdings_table before repeating the insertion process
                self.execute_query(
                    f"""DELETE from etf_holdings_table 
                        WHERE ETF_ticker_ID = {self.__placeholder}
                        and Date = {self.__placeholder};
                    """,
                    (etf_id, self.today)
                )
                # clean up etf_ticker_table before repeating the insertion process
                self.execute_query(
                    f"""DELETE from etf_ticker_table 
                        WHERE ETF_ticker_ID = {self.__placeholder};
                    """,
                    (etf_id,)
                )

            # scrape today's data for the ETF's holdings
            holdings: Mapping[str, Mapping[str, float]] = self.insert_etf_holding_data(
                etf_ticker, 
                scrape_etf_holdings(etf_ticker)
            )
        finally:
            return holdings


        