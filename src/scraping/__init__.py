# standard library dependencies
import logging
logger = logging.getLogger(f"mainLogger.scrape_etf_holdings")
from typing import Mapping
from datetime import datetime

# local dependencies
from .ishares_scraper import FUNDS as ishares_etf_tickers
from .ishares_scraper import fetch as fetch_from_ishares
from .ark_scraper import FUNDS as ark_etf_tickers
from .ark_scraper import fetch as fetch_from_ark
from .invesco_scraper import FUNDS as invesco_etf_tickers
from .invesco_scraper import fetch as fetch_from_invesco
from .zack_scraper import fetch as fetch_from_zack

ishares_etf_tickers = [etf.upper() for etf in ishares_etf_tickers]
ark_etf_tickers = [etf.upper() for etf in ark_etf_tickers]
invesco_etf_tickers = [etf.upper() for etf in invesco_etf_tickers]

def scrape_etf_holdings(etf: str) -> Mapping[str, Mapping[str, float]]:
    """Entrypoint function to iterate over scrapers one-by-one
    until one of them succeeds.

    Parameters
    ----------
    etf : str
        Ticker for the ETF of interest

    Returns
    -------
    Mapping[str, Mapping[str, float]]
        Either an empty dictionary (if no data on the specified ETF was available on zacks.com),
        or a dictionary mapping a holding ticker (string) to a sub-dictionary mapping
        'weight' to the holding ticker's weight in the ETF.

    """
    etf = etf.upper()
    source = "zack"
    start_time = datetime.now()
    try:
        if etf in ishares_etf_tickers:
            logger.info(f"Using ishares scraper to fetch data for {etf}")
            source = "ishares"
            etf_holdings_and_weights = fetch_from_ishares(etf)
        elif etf in ark_etf_tickers:
            logger.info(f"Using ark scraper to fetch data for {etf}")
            source = "ark"
            etf_holdings_and_weights = fetch_from_ark(etf)
        elif etf in invesco_etf_tickers:
            logger.info(f"Using invesco scraper to fetch data for {etf}")
            source = "invesco"
            etf_holdings_and_weights = fetch_from_invesco(etf)
        else:
            logger.info(f"Using zacks.com scraper to fetch data for {etf}")
            etf_holdings_and_weights = fetch_from_zack(etf)
    except Exception as e:
        logger.info(f"Getting updated holdings for ETF: {etf} raised {e}")
        raise e
    try:
        assert len(etf_holdings_and_weights) > 0
    except AssertionError as no_data:
        logger.info(f"Found no data for ETF: {etf} (source: {source})")
        raise no_data
    logger.info(f"Scraping took {datetime.now() - start_time}")
    return etf_holdings_and_weights