# prefetch.py

# standard library dependencies
import time
from datetime import datetime, date
from concurrent.futures import ThreadPoolExecutor, as_completed
# external dependencies

# local dependencies
from src.dbms.PostgresDatabaseClient import PostgresDatabaseClient

def prefetch_etf_data(etf: str) -> bool:
    print(f"Pre-fetching data for {etf}")
    pdc = PostgresDatabaseClient("aws_credentials.json")
    try:
        _ = pdc.get_holdings_and_weights_for_etf(etf.upper())
    except Exception as e:
        print(f"Got an exception when pre-fetching data for {etf}: {e}")
        return False
    else:
        print(f"Successfully pre-fetched data for {etf}")
        return True

def get_latest_update() -> date:
    print(f"Getting latest update date")
    pdc = PostgresDatabaseClient("aws_credentials.json")
    latest = pdc.execute_query("SELECT MAX(Date) FROM etf_holdings_table;")[0][0]
    print(f"Latest update date: {latest}")
    return latest

if __name__ == '__main__':

    last_update = get_latest_update()
    while True:
        day = datetime.now().day
        print(last_update, day)
        if day != last_update:
            last_update = day
            # get known etfs
            pdc = PostgresDatabaseClient("aws_credentials.json")
            known_etfs = pdc.get_known_etfs()
            print(f"Pre-fetching data for {len(known_etfs)} ETFs")
            with ThreadPoolExecutor(max_workers=8) as pool:
                future_to_outcome = {pool.submit(prefetch_etf_data, etf): etf for etf in known_etfs}
                for future in as_completed(future_to_outcome):
                    etf = future_to_outcome[future]
                    try:
                        data = future.result()
                    except Exception as e:
                        print(f'Pre-fetching data for {etf} generated an exception: {e}')
                    else:
                        print(f'Successfully pre-fetched data for {etf}')
        time.sleep(int(60*60))
