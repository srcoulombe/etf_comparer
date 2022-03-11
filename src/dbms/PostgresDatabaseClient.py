# standard library dependencies
import json
import logging
logger = logging.getLogger(f"mainLogger.PostgresDatabaseClient")
import datetime
from datetime import date
from functools import lru_cache
from typing import List, Tuple, Any, Iterable, Union, Mapping

# external dependencies
import boto3
import psycopg2

# local dependencies
from .SQLDatabaseClient import SQLDatabaseClient
from ..scraping import scrape_etf_holdings


def load_credentials(credentials_filepath: str) -> Mapping[str,str]:
    """Wrapper around `json.load` to load a .json file into memory as a dictionary."""
    with open(credentials_filepath, 'r') as handle:
        credentials_dict = json.load(handle)
    return credentials_dict

class PostgresDatabaseClient(SQLDatabaseClient):

    def __init__(   self, 
                    credentials_filepath: str):
        super().__init__(dbms = "postgres")
        self.__credentials = load_credentials(credentials_filepath)
        self.__placeholder = '%s'

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
        session = boto3.Session(
            aws_access_key_id=self.__credentials['AWS_ACCESS_KEY_ID'], 
            aws_secret_access_key=self.__credentials['AWS_SECRET_ACCESS_KEY']
        )
        client = session.client(
            'rds', 
            region_name=self.__credentials['REGION']
        )
        token = client.generate_db_auth_token(
            DBHostname=self.__credentials['ENDPOINT'], 
            Port=self.__credentials['PORT'], 
            DBUsername=self.__credentials['USER'], 
            Region=self.__credentials['REGION']
        )
        # https://www.psycopg.org/psycopg3/docs/basic/usage.html#connection-context
        with psycopg2.connect(
            host=self.__credentials['ENDPOINT'], 
            port=self.__credentials['PORT'], 
            database=self.__credentials['DBNAME'], 
            user=self.__credentials['USER'], 
            password=self.__credentials['PASSWORD'], 
            sslrootcert="SSLCERTIFICATE") as conn:

            cur = conn.cursor()
            cur.execute(query, *args)
        try:
            return cur.fetchall()
        except Exception:
            return None   
    
    def execute_query_over_many_arguments(  self, 
                                            query: str, 
                                            args: Iterable[Any],
                                            get_results: bool = False) -> Union[None,List[Any]]:
        session = boto3.Session(
            aws_access_key_id=self.__credentials['AWS_ACCESS_KEY_ID'], 
            aws_secret_access_key=self.__credentials['AWS_SECRET_ACCESS_KEY']
        )
        client = session.client(
            'rds', 
            region_name=self.__credentials['REGION']
        )
        token = client.generate_db_auth_token(
            DBHostname=self.__credentials['ENDPOINT'], 
            Port=self.__credentials['PORT'], 
            DBUsername=self.__credentials['USER'], 
            Region=self.__credentials['REGION']
        )
        # https://www.psycopg.org/psycopg3/docs/basic/usage.html#connection-context
        # https://stackoverflow.com/questions/8134602/psycopg2-insert-multiple-rows-with-one-query
        results: List[Any] = []
        for arg in args:
            with psycopg2.connect(
                host=self.__credentials['ENDPOINT'], 
                port=self.__credentials['PORT'], 
                database=self.__credentials['DBNAME'], 
                user=self.__credentials['USER'], 
                password=self.__credentials['PASSWORD'], 
                sslrootcert="SSLCERTIFICATE") as conn:

                cur = conn.cursor()
                cur.execute(query, arg)
                if get_results:
                    results.append(cur.fetchall())
        return results if get_results else None

    def insert_etf_holding_data(self,
                                etf_ticker: str,
                                etf_holdings: Mapping[str, Mapping[str, float]]) -> Union[None,List[Tuple[datetime.date, str, str, float]]]:
        if len(etf_holdings) == 0:
            print("PostgresDatabaseClient.insert_data was given an empty mapping; No updates to be made.")
            return None
            
        etf_ticker = etf_ticker.upper()

        # make sure the holdings we scraped are included in `holdings_table`
        # NOTE: this insertion operation doesn't require cleanup if the method
        #       raises an exception afterwards; having more holding tickers in 
        #       `holdings_table` is actually advantageous 
        holding_tickers = [ (holding, ) for holding in etf_holdings.keys() ]

        try:
            logger.info("Inserting holding tickers.")
            self.execute_query_over_many_arguments(
                f"INSERT INTO holdings_table (Holding) VALUES ({self.__placeholder}) ON CONFLICT (Holding) DO NOTHING;",
                holding_tickers
            )
            logger.info("Inserted holding tickers.")
        except Exception as already_exists:
            logger.warning(already_exists)

        for (holding_ticker,) in holding_tickers:
            holding_id: int = self.get_holding_id_for_ticker(holding_ticker)
            # keeping `holding_ticker` and `holding_ticker_id` together is
            # useful for future reference
            etf_holdings[holding_ticker]['holding_ticker_id'] = holding_id

        # then we insert `etf_ticker` into `etf_ticker_table`
        # NOTE: if this raises an exception, we stop immediately
        try:
            logger.info("Inserting etf ticker.")
            self.execute_query(
                f"INSERT INTO etf_ticker_table (ETF_ticker) VALUES ({self.__placeholder}) ON CONFLICT (ETF_ticker) DO NOTHING;",
                (etf_ticker,)
            )
            logger.info(f"successfully inserted {etf_ticker} into etf_ticker_table")
            etf_ticker_id = self.get_etf_id_for_ticker(etf_ticker)
        except Exception as error:
            raise error
        
        holdings = [
            (self.today, etf_ticker_id, holding_dict['holding_ticker_id'], holding_dict['weight'])
            for holding_dict in etf_holdings.values()
        ]
        try:
            logger.info("Inserting into etf_holdings_table.")
            # parameters to insert the scraped data into `etf_holdings_table`
            self.execute_query_over_many_arguments(
                f"""INSERT INTO etf_holdings_table 
                (Date, ETF_ticker_ID, Holding_ID, Holding_Weight)
                VALUES ({self.__placeholder}, {self.__placeholder}, {self.__placeholder}, {self.__placeholder});
                """,
                holdings
            )
            logger.info("Inserted into etf_holdings_table.")
        except Exception as e:
            logger.warning(e)
            self.execute_query(
                f"DELETE FROM etf_ticker_table WHERE ETF_ticker = {self.__placeholder};",
                etf_ticker
            )
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
        # first check if we already have data for the ETF
        try:
            etf_ticker_id = self.get_etf_id_for_ticker(etf_ticker)

        except ValueError as no_current_data_for_etf:
            # if we don't, we attempt to:
            # 1. scrape the most recent holdings data for the etf,
            # 2. insert the holdings' tickers into the `holdings_table`,
            # 3. insert the ETF's ticker into the `etf_ticker_table`, and
            # 4. insert the scraped data into the `etf_holdings_table`.
            # NOTE: if 3 or 4 raise an exception, we crash

            logger.warning(no_current_data_for_etf)
            etf_holdings: Mapping[str, Mapping[str, float]] = scrape_etf_holdings(etf_ticker)
            assert len(etf_holdings) > 0, \
                f"Unable to fetch data for ETF {etf_ticker} on {self.today}"

            holdings = self.insert_etf_holding_data(
                etf_ticker.upper(), 
                etf_holdings
            )
        else:
            holdings: List[Tuple[datetime.date, str, str, float]] = self.execute_query(
                query, 
                (etf_ticker_id, date_)
            )
            try:
                assert len(holdings) > 0
            except AssertionError as present_but_no_date:
                logger.warning(f"{etf_ticker} was present in `ETF_ticker_table` but did not have any holdings data for {date_}; fetching holdings data now.")
                # scrape data
                etf_holdings: Mapping[str, Mapping[str, float]] = scrape_etf_holdings(etf_ticker)
                assert len(etf_holdings) > 0, \
                    f"Unable to fetch data for ETF {etf_ticker} on {self.today}"
                holdings = self.insert_etf_holding_data(
                    etf_ticker.upper(), 
                    etf_holdings
                )
        return holdings
