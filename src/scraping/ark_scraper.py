# standard library dependencies
import csv
from typing import Union, Mapping

# external dependencies
import requests

# The ARK adapter fetches the .csv file of the funds' holdings published on their site
# We're specifically considering the following funds:
# arkk, arkw, arkq, arkf, arkg

FUNDS = ('ARKK', 'ARKW', 'ARKQ', 'ARKF', 'ARKG')

def fetch(  fund: str, 
            headers: Mapping[str,str] = None,
            max_iters: int = 5) -> Union[None, Mapping[str, Mapping[str, float]]]:
    """Scrapes ark-funds.com for today's holdings data on the specified ETF.

    Parameters
    ----------
    etf : str
        ETF of interest.
    headers : Mapping[str,str], optional
        HTTP headers to pass to `urllib.request.Request`. 
        By default `{
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36"
        }`.
    max_iters : int, optional
        Number of successive HTTP requests to submit before accepting a non-200 response
        (ark-funds.com can have strange inconsistencies in their responses to requests).

    Returns
    -------
    Union[None, Mapping[str, Mapping[str, float]]]
        Either None (if no data on the specified ETF was available on zacks.com),
        or a dictionary mapping a holding ticker (string) to a sub-dictionary mapping
        'weight' to the holding ticker's weight in the ETF.
    """
    fund = fund.upper()
    global FUNDS
    assert fund in FUNDS
    if headers is None:
        headers = {
            "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:97.0) Gecko/20100101 Firefox/97.0"
        }
    fund_csv_url = f"https://ark-funds.com/wp-content/uploads/funds-etf-csv/ARK_INNOVATION_ETF_{fund}_HOLDINGS.csv"    
    i = 0
    req = None
    while i < max_iters:
        req = requests.get(
            fund_csv_url, 
            headers=headers
        )
        try:
            req.raise_for_status()
        except requests.exceptions.HTTPError as e:
            print(f"Error: {e}")
            i += 1
        else:
            break
    print(i, req.status_code)
    try:
        req.raise_for_status()
    except requests.exceptions.HTTPError as e:
        # not a 200
        print(f"Error: {e}")
        return dict()
    else:
        data = csv.reader(
            req.content.decode("utf-8").split("\n")
        )
        next(data)
        results = dict()
        for holding in data:
            try:
                ticker = holding[3]
                weight = holding[7]
                if not ticker or not weight:
                    continue
                results[ticker] = round(
                    results.get(ticker, 0) + float(weight.strip('%'))/100,
                    8
                )
            except IndexError:
                continue
                
        return {holding: {'weight':weight} for holding, weight in results.items()}

