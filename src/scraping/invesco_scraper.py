# standard library dependencies
import csv
import urllib.request
from typing import Union, Mapping


FUNDS = [
    "BKLN",
    "DBA",
    "DBB",
    "DBC",
    "DBE",
    "DGL",
    "DBO",
    "DBP",
    "DBS",
    "PDBC",
    "FXA",
    "FXB",
    "FXC",
    "FXE",
    "FXY",
    "FXF",
    "DBV",
    "UDN",
    "UUP",
    "PBP",
    "PHDG",
    "PSP",
    "PSR",
    "KBWY",
    "PSMB",
    "PSMC",
    "PSMG",
    "PSMM",
    "CVY",
    "ADRE",
    "CQQQ",
    "PIZ",
    "PIE",
    "IDLB",
    "PXF",
    "PDN",
    "PXH",
    "PGJ",
    "PIN",
    "IPKW",
    "PID",
    "PBDM",
    "PBEE",
    "ISDX",
    "ISEM",
    "EELV",
    "EEMO",
    "IDHD",
    "IDLV",
    "IDMO",
    "IDHQ",
    "PPA",
    "PZD",
    "PYZ",
    "PEZ",
    "PSL",
    "PXI",
    "PFI",
    "PTH",
    "PRN",
    "PTF",
    "PUI",
    "PBE",
    "PKB",
    "PXE",
    "PBJ",
    "PEJ",
    "PBS",
    "PXQ",
    "PXJ",
    "PJP",
    "PSI",
    "PSJ",
    "PBD",
    "PIO",
    "KBWB",
    "KBWP",
    "KBWR",
    "CUT",
    "PNQI",
    "EWCO",
    "RCD",
    "RHS",
    "RYE",
    "RYF",
    "RYH",
    "RGI",
    "RTM",
    "EWRE",
    "RYT",
    "RYU",
    "CGW",
    "PSCD",
    "PSCC",
    "PSCE",
    "PSCF",
    "PSCH",
    "PSCI",
    "PSCT",
    "PSCM",
    "PSCU",
    "TAN",
    "PHO",
    "PBW",
    "PKW",
    "PCEF",
    "PDP",
    "DWAS",
    "DEF",
    "PFM",
    "DJD",
    "PWB",
    "PWV",
    "PWC",
    "PRF",
    "PRFZ",
    "PGF",
    "IVDG",
    "PEY",
    "KBWD",
    "QQQM",
    "QQQJ",
    "PGX",
    "PBUS",
    "PBSM",
    "QQQ",
    "IUS",
    "IUSS",
    "RYJ",
    "IVRA",
    "USEQ",
    "EQAL",
    "USLB",
    "OMFL",
    "OMFS",
    "EQWL",
    "SPGP",
    "SPMV",
    "RWL",
    "SPVM",
    "SPVU",
    "RSP",
    "SPHB",
    "SPHD",
    "SPLV",
    "SPMO",
    "RPG",
    "RPV",
    "SPHQ",
    "XLG",
    "XRLV",
    "RWK",
    "EWMC",
    "RFG",
    "RFV",
    "XMLV",
    "XMMO",
    "XMHQ",
    "XMVM",
    "RWJ",
    "EWSC",
    "RZG",
    "RZV",
    "XSHD",
    "XSLV",
    "XSMO",
    "XSHQ",
    "XSVM",
    "CSD",
    "RDIV",
    "IVSG",
    "IVLC",
    "VRP",
    "CZA",
    "BSAE",
    "BSBE",
    "BSCE",
    "BSDE",
    "PCY",
    "PGHY",
    "IHYF",
    "PICB",
    "GTO",
    "GSY",
    "PLW",
    "BSCL",
    "BSJL",
    "BSML",
    "BSCM",
    "BSJM",
    "BSMM",
    "BSCN",
    "BSJN",
    "BSMN",
    "BSCO",
    "BSJO",
    "BSMO",
    "BSCP",
    "BSJP",
    "BSMP",
    "BSCQ",
    "BSJQ",
    "BSMQ",
    "BSCR",
    "BSJR",
    "BSMR",
    "BSCS",
    "BSJS",
    "BSMS",
    "BSCT",
    "BSMT",
    "BSCU",
    "BSMU",
    "PWZ",
    "PHB",
    "PFIG",
    "IIGD",
    "IIGV",
    "PZA",
    "PZT",
    "PBTP",
    "PBND",
    "BAB",
    "CLTL",
    "PVI",
    "VRIG",
]
# To get a full list of all Invesco ETFs, navigate to https://www.invesco.com/us/financial-products/etfs/
# and run the following lines of javascript code:
"""
tickers = document.querySelectorAll(".fund-link .ticker-label-bold")
console.log("[" + Array.from(tickers).map(t => '"' + t.textContent.trim() + '"').join(",") + "]")
"""

def fetch(  fund: str, 
            headers: Mapping[str,str] = None) -> Union[None, Mapping[str, Mapping[str, float]]]:
    """Scrapes invesco.com for today's holdings data on the specified ETF.

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

    result: Mapping[str, float] = dict()
    fund_csv_url = f"https://www.invesco.com/us/financial-products/etfs/holdings/main/holdings/0?audienceType=Investor&action=download&ticker={fund}"
    req = urllib.request.Request(
        fund_csv_url, 
        headers=headers
    )
    res = urllib.request.urlopen(req)
    data = csv.reader([l.decode("utf-8") for l in res.readlines()])
    next(data)
    for holding in data:
        try:
            ticker = holding[2].strip()
            weight = holding[5]
            if ticker.startswith("-") or not ticker or not weight:
                continue
            result[ticker] = result.get(ticker, 0) + float(weight)
        except IndexError:
            continue
    return {holding: {'weight':weight} for holding, weight in result.items()}
