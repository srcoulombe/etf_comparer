
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

    @abc.abstractmethod
    def get_holdings_and_weights_for_etf(   self, 
                                            etf_ticker: str,
                                            date_: date = None) -> List[Tuple[datetime.date, str, str, float]]:  
        pass

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
        Tuple[Mapping[str, Mapping[str, Mapping]], List[str]]
            Dictionary mapping an ETF ticker (strings) to a sub-dictionary
            mapping the ETF's holdings (strings) to metadata (e.g. the holding's weight 
            w.r.t. the ETF)., and
            A list of strings indicating the ETF tickers (strings) that weren't available
        
        """
        if date_ is None:
            date_ = self.today
        if date_ > self.today:
            raise ValueError(f"Unable to fetch data from {date_}; Functionality to look into the future is not supported yet.")
            
        results: Mapping[str, Mapping[str, Mapping]] = dict()
        unavailable_etfs: List[str] = []
        for etf_ticker in etf_tickers:
            try:
                etf_ticker_holdings: List[Tuple[datetime.date, str, str, float]] = self.get_holdings_and_weights_for_etf(etf_ticker)
            except AssertionError as etf_is_unfetchable:
                unavailable_etfs.append(etf_ticker)
            else:
                results[etf_ticker] = {
                    holding_ticker: dict(weight=holding_weight)
                    for (date, etf_ticker_id, holding_ticker, holding_weight)
                    in etf_ticker_holdings
                }
        return results, unavailable_etfs                                          