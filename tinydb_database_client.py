# standard library dependencies
from functools import lru_cache
from datetime import datetime, date
from typing import Iterable, Mapping, List

# external dependencies
from tinydb import TinyDB, Query

# local dependencies
from scraping import scrape_etf_holdings

class TinyDBDatabaseClient:
    def __init__(self, db_path: str = "etf_tinydb.json"):
        self.db = TinyDB(db_path)

    def get_known_etfs(self) -> List[str]:
        # NOTE: caching this could be useful
        return [c['name'] for c in self.db.all()]

    def query(  self,
                etfs: Iterable[str],
                date_: date = None,
                headers: Mapping[str,str] = None) -> Mapping[str, Mapping[str, Mapping]]:
        if date_ is None:
            date_ = datetime.now().date()
        if not isinstance(date_, date):
            date_ = datetime.now().date()
        etfs = list(set(etfs))
        etfs_holdings: Mapping[str, List[str]] = dict()
        for etf in etfs:
            assert isinstance(etf, str)
            try:
                data = self.query_(
                    etf,
                    date_ = date_,
                    headers = headers
                )
                etfs_holdings[etf] = data
            except Exception as e:
                print(e)
        return {k:d for k,d in etfs_holdings.items() if len(d) > 0}

    @lru_cache(maxsize = None)
    def query_( self, 
                etf_name: str, 
                date_: date = None):
        if date_ is None:
            date_ = datetime.now().date()
        if not isinstance(date_, date):
            date_ = datetime.now().date()
        etf_holdings = self.db.search(
            Query().name == str(etf_name)
        )
        if len(etf_holdings) == 0:
            try:
                etf_holdings = scrape_etf_holdings(etf_name)
                assert etf_holdings is not None
                assert len(etf_holdings) > 0
            except Exception as e:
                print(f"Unable to fetch data for {etf_name}; {e}")
                return dict()
            else:
                self.db.insert({"name": etf_name, "holdings": etf_holdings})
        else:
            assert len(etf_holdings) == 1, \
                f"found {len(etf_holdings)} records in the database for the etf {etf_name}"
            etf_holdings = etf_holdings[0]['holdings']
        return etf_holdings