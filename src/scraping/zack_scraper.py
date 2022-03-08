# standard library dependencies
import re
from typing import Mapping, Union, List, Tuple

# external dependencies
import requests

def fetch(  etf: str,
            headers: Mapping[str,str] = None) -> Union[None, Mapping[str, Mapping[str, float]]]:
    """Scrapes zacks.com for today's holdings data on the specified ETF.

    Parameters
    ----------
    etf : str
        ETF of interest.
    headers : Mapping[str,str], optional
        HTTP headers to pass to `requests.get`. 
        By default {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:83.0) Gecko/20100101 Firefox/83.0"
        }.

    Returns
    -------
    Union[None, Mapping[str, Mapping[str, float]]]
        Either None (if no data on the specified ETF was available on zacks.com),
        or a dictionary mapping a holding ticker (string) to a sub-dictionary mapping
        'weight' to the holding ticker's weight in the ETF.
    """
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
        etfs_holdings = {holding:{'weight':holding_dict['weight']} for holding, holding_dict in etfs_holdings.items()}
    finally:
        return etfs_holdings