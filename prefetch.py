# prefetch.py

# standard library dependencies
import time
import logging

from datetime import datetime, date
from concurrent.futures import ThreadPoolExecutor, as_completed

# external dependencies

# local dependencies
from src.dbms.PostgresDatabaseClient import PostgresDatabaseClient

def prefetch_etf_data(etf: str) -> bool:
    """Convenience function that creates a PostgresDatabaseClient
    and calls its `.get_holdings_and_weights_for_etf` method to
    scrape and insert the latest holdings data for the provided etf.

    Parameters
    ----------
    etf : str
        Ticker for the etf of interest.

    Returns
    -------
    bool : Boolean confirming whether the operation has been executed
           successfully (True) or not (False).
    
    """
    logging.info(f"Pre-fetching data for {etf}")
    pdc = PostgresDatabaseClient("aws_credentials.json")
    try:
        _ = pdc.get_holdings_and_weights_for_etf(etf.upper())
    except Exception as e:
        logging.error(f"Got an exception when pre-fetching data for {etf}: {e}")
        return False
    else:
        logging.info(f"Successfully pre-fetched data for {etf}")
        return True

def get_latest_update() -> date:
    """Convenience function to fetch the latest date present in the 
    `etf_holdings_table` table of the Postgres database.

    Parameters
    ----------
    None

    Returns
    -------
    date : latest date present in the `etf_holdings_table` table of the Postgres database.
    """
    logging.info(f"Getting latest update date")
    pdc = PostgresDatabaseClient("aws_credentials.json")
    latest = pdc.execute_query("SELECT MAX(Date) FROM etf_holdings_table;")[0][0]
    logging.info(f"Latest update date: {latest}")
    return latest

def prefetch(hibernation_seconds: int = 60*60) -> None:
    """Function that continuously checks the need to fetch
    new ETF holdings data (for all known etfs in the Postgres database)
    every `hibernation_seconds` seconds.

    The fetching operation is only required to happen once per day.

    Parameters
    ----------
    hibernation_seconds : int, optional
        The number of seconds to wait before reconsidering whether new
        data should be fetched. Defaults to 60*60 = 3600 (1 hour).

    Returns
    -------
    None
    """
    hibernation_seconds = max(60*60, hibernation_seconds)
    last_update = get_latest_update()
    while True:
        day = datetime.now().day
        #print(last_update, day)
        if day != last_update:
            last_update = day
            # get known etfs
            pdc = PostgresDatabaseClient("aws_credentials.json")
            known_etfs = pdc.get_known_etfs()
            logging.info(f"Pre-fetching data for {len(known_etfs)} ETFs")
            with ThreadPoolExecutor(max_workers=8) as pool:
                future_to_outcome = {pool.submit(prefetch_etf_data, etf): etf for etf in known_etfs}
                for future in as_completed(future_to_outcome):
                    etf = future_to_outcome[future]
                    try:
                        data = future.result()
                        assert data
                    except (Exception, AssertionError) as e:
                        logging.error(f'Pre-fetching data for {etf} generated an exception: {e}')
                    else:
                        logging.info(f'Prefetch operation for {etf} concluded successfully: {data}')
            logging.info("Concluded prefetch operations for all {len(known_etfs)} known etfs; hibernating for 1 hour.")
        else:
            logging.info("No need for prefetching as of now; hibernating for 1 hour.")
        time.sleep(int(hibernation_seconds))

if __name__ == '__main__':
    logging.basicConfig(format='[%(asctime)s] - %(levelname)s:%(message)s', level=logging.INFO)
    prefetch()
