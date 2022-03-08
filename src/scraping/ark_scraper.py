# standard library dependencies
import csv
import urllib.request
from typing import Union, Mapping

# The ARK adapter fetches the .csv file of the funds' holdings published on their site
# We're specifically considering the following funds:
# arkk, arkw, arkq, arkf, arkg

FUNDS = ('ARKK', 'ARKW', 'ARKQ', 'ARKF', 'ARKG')

def fetch(  fund: str, 
            headers: Mapping[str,str] = None) -> Union[None, Mapping[str, Mapping[str, float]]]:
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
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36"
        }

    fund_csv_url = f"https://ark-funds.com/wp-content/uploads/funds-etf-csv/ARK_INNOVATION_ETF_{fund}_HOLDINGS.csv"
    result: Mapping[str, float] = dict()

    req = urllib.request.Request(
        fund_csv_url, 
        headers=headers
    )
    res = urllib.request.urlopen(req)
    data = csv.reader(
        [l.decode("utf-8") for l in res.readlines()]
    )
    next(data)
    for holding in data:
        try:
            ticker = holding[3]
            weight = holding[7]
            if not ticker or not weight:
                continue
            result[ticker] = result.get(ticker, 0) + float(weight.strip('%'))/100
        except IndexError:
            continue
    return {holding: {'weight':weight} for holding, weight in result.items()}
