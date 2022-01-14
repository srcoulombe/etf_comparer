
# standard library dependencies
import abc
from datetime import datetime, date
from functools import lru_cache
from typing import List, Union, Tuple, Mapping, Any, Iterable

# local dependencies
from ..scraping import scrape_etf_holdings


class SQLDatabaseClient(abc.ABC):
    """Base class for SQL-based database management system client classes"""
    def __init__(self, dbms: str = "sqlite3"):
        dbms = dbms.lower()
        assert dbms in ("postgres", "sqlite3"), \
            f"SQLDatabaseClient only supports 'postgres' and 'sqlite3'; {dbms} is unsupported at the moment."
        self.__dbms = dbms
        self.__placeholder = '%s' if self.__dbms == 'postgres' else '?'

    @property
    def today(self) -> datetime.date:
        return datetime.now().date()

    @abc.abstractclassmethod
    def execute_query(self, query: str, *args) -> List[Any]:
        pass

    @abc.abstractclassmethod
    def execute_query_over_many_arguments(self, query: str, args: Iterable[Any]):
        pass
    
    @abc.abstractproperty
    def holdings_table_creation_query(self) -> str:
        pass

    @abc.abstractproperty
    def etf_ticker_table_creation_query(self) -> str:
        pass

    @abc.abstractproperty
    def etf_table_creation_query(self) -> str:
        pass
    
    def create_holdings_table(self) -> None:
        self.execute_query(self.holdings_table_creation_query)

    def create_etf_ticker_table(self) -> None:
        self.execute_query(self.etf_ticker_table_creation_query)

    def create_etf_table(self) -> None:
        self.execute_query(self.etf_table_creation_query)

    def setup(self) -> None:
        """Convenience method to setup the database and required tables"""
        self.create_holdings_table()
        self.create_etf_ticker_table()
        self.create_etf_table()

    def get_known_etfs(self) -> List[str]:
        """Returns the list of known ETFs in the database."""
        res = self.execute_query(
            "SELECT ETF_ticker from etf_ticker_table;",
        )
        return [ ticker for (ticker, ) in res ]

    def get_etf_id_for_ticker(  self,
                                etf_ticker: str) -> int:
        """Returns the numerical ID associated with the provided `etf_ticker`
        if it exists in the `etf_ticker_table`.

        Parameters
        ----------
        etf_ticker : str
            ETF ticker of interest.

        Returns
        -------
        int
            Numerical ID associated with `etf_ticker`

        Raises
        ------
        ValueError
            If the `etf_ticker` is not present in `etf_ticker_table`.
        """
        #Returns the numerical ID for the specified `etf_ticker` (string).
        etf_ticker = etf_ticker.upper()
        etf_ids: List[Tuple[int]] = self.execute_query(
            f"SELECT ETF_ticker_ID FROM etf_ticker_table WHERE ETF_ticker = {self.__placeholder};", 
            (etf_ticker,)
        )
        try:     
            etf_id: int = etf_ids[0][0]
        except IndexError as no_data_for_etf:
            raise ValueError(f"No data for ETF: '{etf_ticker}'") from no_data_for_etf
        else:
            return etf_id 

    def get_holding_id_for_ticker(  self, 
                                    holding_ticker: str) -> int:
        """Returns the numerical ID associated with the provided `holding_ticker`
        in `holdings_table`. If the `holding_ticker` does not exist, it is inserted
        in said table.

        Parameters
        ----------
        holding_ticker : str
            Ticker for the holding of interest.

        Returns
        -------
        int
            Numerical ID associated with `holding_ticker`
        """
        holding_ticker = holding_ticker.upper()
        try:
            holding_id: int = self.execute_query(
                f"SELECT Holding_ID from holdings_table where Holding = {self.__placeholder};",
                (holding_ticker,)
            )[0][0]
            return holding_id
        except Exception as not_in_table_yet:
            self.execute_query(
                f"INSERT INTO holdings_table (Holding) VALUES ({self.__placeholder});",
                (holding_ticker,)
            )
            holding_id: int = self.execute_query(
                f"SELECT Holding_ID from holdings_table where Holding = {self.__placeholder};",
                (holding_ticker,)
            )[0][0]
            return holding_id

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
                print(f"Holdings for ETF '{etf_ticker}' were incomplete; updating...")
                
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
            etf_holdings: Mapping[str, Mapping[str, float]] = scrape_etf_holdings(etf_ticker)
            assert len(etf_holdings) > 0
            
            holding_tickers = [ (holding, ) for holding in etf_holdings.keys() ]
            
            # NOTE: this insertion operation doesn't require cleanup if the method
            #       raises an exception afterwards; having more holding tickers in 
            #       `holding_table` is actually advantageous
            try:
                self.execute_query_over_many_arguments(
                    f"insert into holdings_table (Holding) values ({self.__placeholder} );",
                    holding_tickers
                )
            except Exception as already_exists:
                print(already_exists)
                
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
                self.execute_query(
                    #"insert or ignore into etf_ticker_table (ETF_ticker) values (%s);",
                    f"insert into etf_ticker_table (ETF_ticker) values ({self.__placeholder});",
                    (etf_ticker,)
                )
            except Exception as already_exists:
                # log
                print(already_exists)

            etf_ticker_id = self.get_etf_id_for_ticker(etf_ticker)

            params = [
                (self.today, etf_ticker_id, holding_dict['holding_ticker_id'], holding_dict['weight'])
                for holding_dict in etf_holdings.values()
            ]
            try:
                self.execute_query_over_many_arguments(
                    f"""insert into etf_holdings_table 
                    (Date, ETF_ticker_ID, Holding_ID, Holding_Weight)
                    values ({self.__placeholder}, {self.__placeholder}, {self.__placeholder}, {self.__placeholder});
                    """,
                    params
                )
            except Exception as error_in_insertion:
                # log
                self.execute_query(
                    f"DELETE FROM etf_ticker_table WHERE ETF_ticker = %{self.__placeholder};",
                    (etf_ticker,)
                )
                print(f"Caught error in inserting {etf_ticker} in `etf_holdings_table`: {error_in_insertion}")

            else:
                holdings: List[Tuple[datetime.date, str, str, float]] = self.execute_query(
                    query, 
                    (etf_ticker_id, self.today)
                )
        return holdings

    def get_holdings_and_weights_for_etfs(  self, 
                                            etf_tickers: List[str],
                                            date_: date = None) -> Mapping[str, Mapping[str, Mapping]]:
        """Wrapper to execute `get_holdings_and_weights_for_etf` over all ETF tickers provided in `etfs`. 

        Parameters
        ----------
        etf_tickers : Iterable[str]
            Iterable of tickers for the ETFs of interest.
        date_ : date, optional
            `datetime.date` object representing the date of interest.
            Defaults None, which gets replaced by today's date.
        
        Returns
        -------
        Mapping[str, Mapping[str, Mapping]]
            Dictionary mapping an ETF ticker (strings) to a sub-dictionary
            mapping the ETF's holdings (strings) to metadata (e.g. the holding's weight 
            w.r.t. the ETF).
        """
        if date_ is None:
            date_ = self.today
        if date_ > self.today:
            raise ValueError(f"Unable to fetch data from {date_}; Functionality to look into the future is not supported yet.")
            
        results: Mapping[str, Mapping[str, Mapping]] = dict()
        for etf_ticker in etf_tickers:
            etf_ticker_holdings: List[Tuple[datetime.date, str, str, float]] = self.get_holdings_and_weights_for_etf(etf_ticker)
            results[etf_ticker] = {
                holding_ticker: dict(weight=holding_weight)
                for (date, etf_ticker_id, holding_ticker, holding_weight)
                in etf_ticker_holdings
            }
        return results                                            