# standard library dependencies
import logging
logger = logging.getLogger(f"mainLogger.TinyDBDatabaseClient")
from functools import lru_cache
from datetime import datetime, date
from typing import Iterable, Mapping, List, Tuple

# external dependencies
from tinydb import TinyDB, Query

# local dependencies
from ..scraping import scrape_etf_holdings

class TinyDBDatabaseClient:
    """TinyDB database client.
    """
    def __init__(self, db_path: str = "data/etf_tinydb.json"):
        self.db = TinyDB(db_path)

    @property
    def today(self) -> str:
        """Returns today's date as a yyyy-mm-dd formatted string
        (TinyDB does not support inserting of objects that aren't json-serializable)."""
        return str(datetime.now().date())

    def get_known_etfs(self) -> List[str]:
        """Returns the list of known ETFs in the database."""
        return [c['name'] for c in self.db.all()]

    def scrape_and_insert_etf_holding_data( self, 
                                            etf_name: str,
                                            date_: date = None) -> Mapping[str, Mapping[str, Mapping]]:
        """Function that attempts to scrape holdings data for the ETF specified
        by `etf_name` and insert it into the TinyDB database.

        Parameters
        ----------
        etf_name : str
            Ticker for the ETF of interest.
        date_ : str, optional
            `yyyy-mm-dd` formatted string representing the date of interest.
            Defaults None, which gets replaced by today's date.
        
        Returns
        -------
        Mapping[str, Mapping[str, Mapping]]
            Dictionary mapping the ETF's ticker (`etf_name`) to a dictionary
            mapping the ETF's holdings (strings) to metadata (e.g. the holding's weight 
            w.r.t. the ETF).
        """
        if date_ is None:
            date_ = self.today
        if datetime.strptime(date_, '%Y-%m-%d') > datetime.strptime(self.today, '%Y-%m-%d'):
            raise ValueError(f"Unable to fetch data from {date_}; Functionality to look into the future is not supported yet.")
        etf_name = etf_name.upper()
        try:
            etf_holdings = scrape_etf_holdings(etf_name)
            assert etf_holdings is not None
            assert len(etf_holdings) > 0
        except Exception as e:
            message = f"Unable to fetch data for {etf_name}; {e}"
            logger.warning(message)
            raise ValueError(message) from e
        else:
            self.db.insert({
                "name": etf_name, 
                "holdings": etf_holdings,
                "date": date_
            })
            return etf_holdings

    @lru_cache(maxsize = None)
    def get_holdings_and_weights_for_etf(   self, 
                                            etf_name: str,
                                            date_: str = None) -> Mapping[str, Mapping[str, Mapping]]:
        """LRU-cached method to fetch holding and weights data for the 
        ETF specified by the `etf_name` ticker from the TinyDB database. 
        If no data for said ETF is present, this function will attempt to scrape
        (see `..scraping.scrape_etf_holdings`) and insert it into the TinyDB database.

        Parameters
        ----------
        etf_name : str
            Ticker for the ETF of interest.
        date_ : str, optional
            `yyyy-mm-dd` formatted string representing the date of interest.
            Defaults None, which gets replaced by today's date.
        
        Returns
        -------
        Mapping[str, Mapping[str, Mapping]]
            Dictionary mapping the ETF's ticker (`etf_name`) to a dictionary
            mapping the ETF's holdings (strings) to metadata (e.g. the holding's weight 
            w.r.t. the ETF).
        """
        if date_ is None:
            date_ = self.today
        if datetime.strptime(date_, '%Y-%m-%d') > datetime.strptime(self.today, '%Y-%m-%d'):
            raise ValueError(f"Unable to fetch data from {date_}; Functionality to look into the future is not supported yet.")
        etf_name = etf_name.upper()

        etf_holdings = self.db.search(
            (Query().name == str(etf_name)) \
            & (Query().date == date_)
        )
        if len(etf_holdings) == 0:
            if date_ != self.today:
                raise ValueError(f"No data is available for {etf_name} on {date_}")
            logger.info(f"Attempting to scrape and insert holdings data for {etf_name}")
            etf_holdings = self.scrape_and_insert_etf_holding_data(
                etf_name,
                date_ = date_
            )
        else:
            assert len(etf_holdings) == 1, \
                f"Found {len(etf_holdings)} records in the database for the etf {etf_name}"
            try:
                assert len(etf_holdings[0]['holdings']) > 0
            except (AssertionError, Exception) as ae_e:
                logger.info(f"Found incomplete data for {etf_name}")
                logger.info(f"Attempting to scrape and insert holdings data for {etf_name}")
                etf_holdings = self.scrape_and_insert_etf_holding_data(
                    etf_name,
                    date_ = date_
                )
            etf_holdings = etf_holdings[0]['holdings']
        return etf_holdings

    def get_holdings_and_weights_for_etfs(  self,
                                            etfs: Iterable[str],
                                            date_: str = None) -> Tuple[Mapping[str, Mapping[str, Mapping]], List[str]]:
        """Wrapper to execute `get_holdings_and_weights_for_etf` over all ETF tickers provided in `etfs`. 

        Parameters
        ----------
        etfs : Iterable[str]
            Iterable of tickers for the ETFs of interest.
        date_ : str, optional
            `yyyy-mm-dd` formatted string representing the date of interest.
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
        if datetime.strptime(date_, '%Y-%m-%d') > datetime.strptime(self.today, '%Y-%m-%d'):
            raise ValueError(f"Unable to fetch data from {date_}; Functionality to look into the future is not supported yet.")
        
        etfs = list(set(etfs))
        etfs_holdings: Mapping[str, List[str]] = dict()
        unavailable_etfs: List[str] = []
        for etf in etfs:
            assert isinstance(etf, str)
            try:
                data = self.get_holdings_and_weights_for_etf(
                    etf,
                    date_ = date_
                )
                etfs_holdings[etf] = data
            except Exception as e:
                # log
                logger.warning(e)
                unavailable_etfs.append(etf)
        return {k:d for k,d in etfs_holdings.items() if len(d) > 0}, unavailable_etfs

