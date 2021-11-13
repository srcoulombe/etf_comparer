# backend.py 
# TODO: rename this module

# standard library dependencies
import re
from functools import lru_cache
from datetime import datetime, date
from typing import Iterable, Mapping, Union, List

# external dependencies
import requests
from tinydb import TinyDB, Query

@lru_cache(maxsize = None)
def download_etf_holdings(  etf: str,
                            headers: Mapping[str,str] = None) -> Union[None, List[str]]:
    if headers is None:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:83.0) Gecko/20100101 Firefox/83.0"
        }
    
    r = requests.get(
        f"https://www.zacks.com/funds/etf/{etf}/holding",
        headers = headers
    )
    try:
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        # not a 200
        print(f"Error: {e}")
        etfs_holdings = None
    else:
        pat = re.compile(r'etf\\\/(.*?)\\')
        pat = re.compile(r'<span class=\\"hoverquote-symbol\\">([a-zA-Z]*?)<span class=\\"sr-only\\"><\\/span><\\/span><\\/a>", "([0-9,]+)", "([0-9,\.]+)", "([0-9,\.]+)"')
        etfs_holdings = dict()
        for (ticker_symbol, str_shares, str_weight, str_52_week_change) in re.findall(pat, r.text):
            etfs_holdings[ticker_symbol] = {
                'shares': int(str_shares.replace(",","")),
                'weight': float(str_weight),
                '52_week_change': float(str_52_week_change)
            }
    finally:
        return etfs_holdings


class DatabaseClient:
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
                date_: date = None,
                headers: Mapping[str,str] = None):
        if date_ is None:
            date_ = datetime.now().date()
        if not isinstance(date_, date):
            date_ = datetime.now().date()
        etf_holdings = self.db.search(
            Query().name == str(etf_name)
        )
        if len(etf_holdings) == 0:
            try:
                etf_holdings = download_etf_holdings(
                    etf_name,
                    headers = headers
                )
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