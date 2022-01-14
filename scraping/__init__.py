# standard library dependencies
from typing import List, Tuple, Mapping

# local dependencies
from .ishares_scraper import FUNDS as ishares_etf_tickers
from .ishares_scraper import fetch as fetch_from_ishares
from .ark_scraper import FUNDS as ark_etf_tickers
from .ark_scraper import fetch as fetch_from_ark
from .invesco_scraper import FUNDS as invesco_etf_tickers
from .invesco_scraper import fetch as fetch_from_invesco
from .zack_scraper import fetch as fetch_from_zack

ishares_etf_tickers = [etf.lower() for etf in ishares_etf_tickers]
ark_etf_tickers = [etf.lower() for etf in ark_etf_tickers]
invesco_etf_tickers = [etf.lower() for etf in invesco_etf_tickers]

def scrape_etf_holdings(etf: str) -> Mapping[str, Mapping[str, float]]:
    etf = etf.lower()
    source = "zack"
    try:
        if etf in ishares_etf_tickers:
            source = "ishares"
            etf_holdings_and_weights = fetch_from_ishares(etf)
        elif etf in ark_etf_tickers:
            source = "ark"
            etf_holdings_and_weights = fetch_from_ark(etf)
        elif etf in invesco_etf_tickers:
            source = "invesco"
            etf_holdings_and_weights = fetch_from_invesco(etf)
        else:
            etf_holdings_and_weights = fetch_from_zack(etf)
    except Exception as e:
        print(f"getting updated holdings for ETF: {etf} raised {e}")
        raise e
    try:
        assert len(etf_holdings_and_weights) > 0
    except AssertionError as no_data:
        print(f"Found no data for ETF: {etf} (source: {source})")
        raise no_data
    return etf_holdings_and_weights