# standard library dependencies
import csv
from typing import Mapping

# external dependencies
import requests

FUNDS = [
    'ADRE',
    'BKLN',
    'BKLN',
    'CGW',
    'CQQQ',
    'CSD',
    'CUT',
    'CVY',
    'CZA',
    'DEF',
    'DJD',
    'DWAS',
    'EELV',
    'EEMO',
    'EQAL',
    'EQWL',
    'EWCO',
    'EWMC',
    'EWRE',
    'EWSC',
    'IDHD',
    'IDHQ',
    'IDLB',
    'IDLV',
    'IDMO',
    'IPKW',
    'ISDX',
    'ISEM',
    'IUS',
    'IUSS',
    'IVDG',
    'IVLC',
    'IVRA',
    'IVSG',
    'KBWB',
    'KBWR',
    'OMFL',
    'OMFS',
    'PBD',
    'PBDM',
    'PBE',
    'PBEE',
    'PBJ',
    'PBP',
    'PBS',
    'PBSM',
    'PBUS',
    'PBW',
    'PCEF',
    'PDN',
    'PDP',
    'PEJ',
    'PEY',
    'PEZ',
    'PFI',
    'PFM',
    'PGJ',
    'PHDG',
    'PHO',
    'PID',
    'PIE',
    'PIN',
    'PIO',
    'PIZ',
    'PJP',
    'PKB',
    'PKW',
    'PNQI',
    'PPA',
    'PRF',
    'PRFZ',
    'PRN',
    'PSCD',
    'PSCF',
    'PSCH',
    'PSCI',
    'PSCM',
    'PSCT',
    'PSCU',
    'PSI',
    'PSJ',
    'PSL',
    'PSMB',
    'PSMC',
    'PSMG',
    'PSMM',
    'PSP',
    'PSR',
    'PTF',
    'PTH',
    'PUI',
    'PWB',
    'PWC',
    'PWV',
    'PXE',
    'PXF',
    'PXH',
    'PXI',
    'PXJ',
    'PXQ',
    'PYZ',
    'QQQ',
    'QQQJ',
    'QQQM',
    'RCD',
    'RDIV',
    'RFG',
    'RFV',
    'RGI',
    'RHS',
    'RPG',
    'RPV',
    'RSP',
    'RTM',
    'RWJ',
    'RWK',
    'RWL',
    'RYE',
    'RYF',
    'RYH',
    'RYJ',
    'RYT',
    'RYU',
    'RZG',
    'RZV',
    'SPGP',
    'SPHB',
    'SPHD',
    'SPHQ',
    'SPLV',
    'SPMO',
    'SPMV',
    'SPVM',
    'SPVU',
    'TAN',
    'USEQ',
    'USLB',
    'XLG',
    'XMHQ',
    'XMLV',
    'XMMO',
    'XMVM',
    'XRLV',
    'XSHD',
    'XSHQ',
    'XSLV',
    'XSMO',
    'XSVM'
]
# To get a full list of all Invesco ETFs, navigate to https://www.invesco.com/us/financial-products/etfs/
# and run the following lines of javascript code:
"""
tickers = document.querySelectorAll(".fund-link .ticker-label-bold")
console.log("[" + Array.from(tickers).map(t => '"' + t.textContent.trim() + '"').join(",") + "]")
"""

def fetch(  fund: str, 
            headers: Mapping[str,str] = None) -> Mapping[str, Mapping[str, float]]:
    """Scrapes invesco.com for today's holdings data on the specified ETF.

    Parameters
    ----------
    etf : str
        ETF of interest.
    headers : Mapping[str,str], optional
        HTTP headers to pass to `urllib.request.Request`. 
        By default `{
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:83.0) Gecko/20100101 Firefox/83.0"
        }`.

    Returns
    -------
    Mapping[str, Mapping[str, float]]
        Either an empty dictionary (if no data on the specified ETF was available on zacks.com),
        or a dictionary mapping a holding ticker (string) to a sub-dictionary mapping
        'weight' to the holding ticker's weight in the ETF.
    """
    fund = fund.upper()
    global FUNDS
    assert fund in FUNDS
    if headers is None:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:83.0) Gecko/20100101 Firefox/83.0"
        }

    result: Mapping[str, float] = dict()
    fund_csv_url = f"https://www.invesco.com/us/financial-products/etfs/holdings/main/holdings/0?audienceType=Investor&action=download&ticker={fund}"
    req = requests.get(
        fund_csv_url, 
        headers=headers
    )
    try:
        req.raise_for_status()
    except requests.exceptions.HTTPError as e:
        # not a 200
        print(f"Error: {e}")
    else:
        data = csv.reader(
            req.content.decode("utf-8").split("\n")
        )
        holdings_data = dict()
        next(data)
        for holding in data:
            try:
                ticker = holding[2].strip()
                weight = holding[5]
                if ticker.startswith("-") or not ticker or not weight:
                    continue
                holdings_data[ticker] = holdings_data.get(ticker, 0) + float(weight)
            except IndexError:
                continue
        result = {holding: {'weight':weight} for holding, weight in holdings_data.items()}
    finally:
        return result
