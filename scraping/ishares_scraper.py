import csv
import urllib.request
from typing import Mapping, List, Tuple, Union

# The iShares adapter fetches the .csv file of the funds' holdings published on their site
# AFAIK there's no way to do this programmatically for any fund so we need to manually
# add the unique URL for each fund's file (see get_fund_file() function below)

# These two only work as XLS not CSV. (Also not really ETFs) They are excluded from the list below
# "slv": "/us/products/239855/ishares-silver-trust-fund/1521942788811.ajax?fileType=xls&fileName=iShares-Silver-Trust_fund&dataType=fund"
# "iau": "/us/products/239561/ishares-gold-trust-fund/1521942788811.ajax?fileType=xls&fileName=iShares-Gold-Trust_fund&dataType=fund"


FUNDS = ["aaxj", "acwf", "acwi", "acwv", "acwx", "agg", "agt", "agz", "aia", "amca", 
        "aoa", "aok", "aom", "aor", "bftr", "bgrn", "bkf", "bmed", "btek", "byld", "ccrv", 
        "cemb", "cmbs", "cmdy", "cmf", "cnya", "comt", "crbn", "defa", "dgro", "divb", 
        "dmxf", "dsi", "dvy", "dvya", "dvye", "dynf", "eagg", "eaoa", "eaok", "eaom", 
        "eaor", "ech", "ecns", "eden", "eem", "eema", "eems", "eemv", "efa", "efav", 
        "efg", "efnl", "efv", "eido", "eirl", "eis", "emb", "embh", "emgf", "emhy", 
        "emif", "emxc", "emxf", "enor", "enzl", "ephe", "epol", "epp", "epu", "erus", 
        "esgd", "esge", "esgu", "esml", "eufn", "eusa", "eusb", "ewa", "ewc", "ewd", 
        "ewg", "ewgs", "ewh", "ewi", "ewj", "ewje", "ewjv", "ewk", "ewl", "ewm", "ewn", 
        "ewo", "ewp", "ewq", "ews", "ewt", "ewu", "ewus", "eww", "ewy", "ewz", "ewzs", 
        "exi", "eza", "ezu", "faln", "fibr", "fill", "flot", "fm", "fovl", "fxi", "gbf", 
        "ghyg", "gnma", "govt", "govz", "gsg", "gvi", "hawx", "hdv", "heem", "hefa", 
        "hewc", "hewg", "hewj", "hewu", "heww", "hezu", "hjpx", "hscz", "hybb", "hydb", 
        "hyg", "hygh", "hyxf", "hyxu", "iagg", "iai", "iak", "iat", "iauf", "ibb", "ibce", 
        "ibdd", "ibdm", "ibdn", "ibdo", "ibdp", "ibdq", "ibdr", "ibds", "ibdt", "ibdu", 
        "ibdv", "ibha", "ibhb", "ibhc", "ibhd", "ibhe", "ibhf", "ibmj", "ibmk", "ibml", 
        "ibmm", "ibmn", "ibmo", "ibmp", "ibmq", "ibta", "ibtb", "ibtd", "ibte", "ibtf", 
        "ibtg", "ibth", "ibti", "ibtj", "ibtk", "icf", "icln", "icol", "icsh", "icvt", 
        "idev", "idna", "idrv", "idu", "idv", "iecs", "iedi", "ief", "iefa", "iefn", 
        "iehs", "iei", "ieih", "ieme", "iemg", "ieo", "ietc", "ieur", "ieus", "iev", 
        "iez", "ifgl", "ifra", "igbh", "ige", "igeb", "igf", "igib", "iglb", "igm", 
        "ign", "igov", "igro", "igsb", "igv", "ihak", "ihe", "ihf", "ihi", "ijh", "ijj", 
        "ijk", "ijr", "ijs", "ijt", "ilf", "iltb", "imtb", "imtm", "inda", "indy", "intf", 
        "ioo", "ipac", "ipff", "iqlt", "irbo", "iscf", "ishg", "istb", "isze", "ita", "itb", 
        "itot", "iusb", "iusg", "iusv", "ive", "ivlu", "ivv", "ivw", "iwb", "iwc", "iwd", 
        "iwf", "iwfh", "iwl", "iwm", "iwn", "iwo", "iwp", "iwr", "iws", "iwv", "iwx", "iwy", 
        "ixc", "ixg", "ixj", "ixn", "ixp", "ixus", "iyc", "iye", "iyf", "iyg", "iyh", "iyj", 
        "iyk", "iyld", "iym", "iyr", "iyt", "iyw", "iyy", "iyz", "jkd", "jke", "jkf", "jkg", 
        "jkh", "jki", "jkj", "jkk", "jkl", "jpxn", "jxi", "ksa", "kwt", "kxi", "ldem", 
        "lemb", "lqd", "lqdh", "lqdi", "lrgf", "mbb", "mchi", "mear", "midf", "mtum", 
        "mub", "mxi", "near", "nyf", "oef", "pff", "pick", "qat", "qlta", "qual", "reet", 
        "rem", "rez", "ring", "rxi", "scj", "scz", "sdg", "sgov", "shv", "shy", "shyg", 
        "size", "slqd", "slvp", "smin", "smlf", "smmd", "smmv", "soxx", "stip", "stlc", 
        "stlg", "stlv", "stmb", "stsb", "sub", "susa", "susb", "susc", "susl", "sval", 
        "tecb", "tflo", "thd", "tip", "tlh", "tlt", "tok", "tur", "uae", "urth", "ushy", 
        "usig", "usmv", "usrt", "usxf", "vegi", "vlue", "wood", "wps", "xjh", "xjr", "xt", "xvv"]

def get_fund_file(symbol: str) -> str:
    symbol = symbol.lower()
    funds_basepaths = {
        "aaxj": "/239601/ishares-msci-all-country-asia-ex-japan-etf/1467271812596.ajax",
        "acwf": "/272821/ishares-msci-global-multi-factor-etf/1467271812596.ajax",
        "acwi": "/239600/ishares-msci-acwi-etf/1467271812596.ajax",
        "acwv": "/239605/ishares-msci-all-country-world-minimum-volatility-etf/1467271812596.ajax",
        "acwx": "/239594/ishares-msci-acwi-ex-us-etf/1467271812596.ajax",
        "agg": "/239458/ishares-core-total-us-bond-market-etf/1467271812596.ajax",
        "agt": "/287286/fund/1467271812596.ajax",
        "agz": "/239457/ishares-agency-bond-etf/1467271812596.ajax",
        "aia": "/239730/ishares-asia-50-etf/1467271812596.ajax",
        "amca": "/290127/fund/1467271812596.ajax",
        "aoa": "/239729/ishares-aggressive-allocation-etf/1467271812596.ajax",
        "aok": "/239733/ishares-conservative-allocation-etf/1467271812596.ajax",
        "aom": "/239765/ishares-moderate-allocation-etf/1467271812596.ajax",
        "aor": "/239756/ishares-growth-allocation-etf/1467271812596.ajax",
        "bftr": "/316005/fund/1467271812596.ajax",
        "bgrn": "/305296/fund/1467271812596.ajax",
        "bkf": "/239614/ishares-msci-bric-etf/1467271812596.ajax",
        "bmed": "/316007/fund/1467271812596.ajax",
        "btek": "/316011/fund/1467271812596.ajax",
        "byld": "/264127/ishares-yield-optimized-bond-etf/1467271812596.ajax",
        "ccrv": "/310784/fund/1467271812596.ajax",
        "cemb": "/239525/ishares-emerging-markets-corporate-bond-etf/1467271812596.ajax",
        "cmbs": "/239459/ishares-cmbs-etf/1467271812596.ajax",
        "cmdy": "/292741/fund/1467271812596.ajax",
        "cmf": "/239731/ishares-california-amtfree-muni-bond-etf/1467271812596.ajax",
        "cnya": "/273318/ishares-msci-china-a-etf/1467271812596.ajax",
        "comt": "/270319/ishares-commodity-etf/1467271812596.ajax",
        "crbn": "/271054/ishares-msci-acwi-low-carbon-target-etf/1467271812596.ajax",
        "defa": "/280769/ishares-adaptive-currency-hedged-msci-eafe-etf/1467271812596.ajax",
        "dgro": "/264623/ishares-core-dividend-growth-etf/1467271812596.ajax",
        "divb": "/291387/fund/1467271812596.ajax",
        "dmxf": "/314362/fund/1467271812596.ajax",
        "dsi": "/239667/ishares-msci-kld-400-social-etf/1467271812596.ajax",
        "dvy": "/239500/ishares-select-dividend-etf/1467271812596.ajax",
        "dvya": "/239443/ishares-asiapacific-dividend-etf/1467271812596.ajax",
        "dvye": "/239526/ishares-emerging-markets-dividend-etf/1467271812596.ajax",
        "dynf": "/307283/fund/1467271812596.ajax",
        "eagg": "/305252/fund/1467271812596.ajax",
        "eaoa": "/314414/fund/1467271812596.ajax",
        "eaok": "/314417/fund/1467271812596.ajax",
        "eaom": "/314411/fund/1467271812596.ajax",
        "eaor": "/314409/fund/1467271812596.ajax",
        "ech": "/239618/ishares-msci-chile-capped-etf/1467271812596.ajax",
        "ecns": "/239620/ishares-msci-china-smallcap-etf/1467271812596.ajax",
        "eden": "/239621/ishares-msci-denmark-capped-etf/1467271812596.ajax",
        "eem": "/239637/ishares-msci-emerging-markets-etf/1467271812596.ajax",
        "eema": "/239629/ishares-msci-emerging-markets-asia-etf/1467271812596.ajax",
        "eems": "/239642/ishares-msci-emerging-markets-smallcap-etf/1467271812596.ajax",
        "eemv": "/239641/ishares-msci-emerging-markets-minimum-volatility-etf/1467271812596.ajax",
        "efa": "/239623/ishares-msci-eafe-etf/1467271812596.ajax",
        "efav": "/239626/ishares-msci-eafe-minimum-volatility-etf/1467271812596.ajax",
        "efg": "/239622/ishares-msci-eafe-growth-etf/1467271812596.ajax",
        "efnl": "/239647/ishares-msci-finland-capped-etf/1467271812596.ajax",
        "efv": "/239628/ishares-msci-eafe-value-etf/1467271812596.ajax",
        "eido": "/239661/ishares-msci-indonesia-etf/1467271812596.ajax",
        "eirl": "/239662/ishares-msci-ireland-capped-etf/1467271812596.ajax",
        "eis": "/239663/ishares-msci-israel-capped-etf/1467271812596.ajax",
        "emb": "/239572/ishares-jp-morgan-usd-emerging-markets-bond-etf/1467271812596.ajax",
        "embh": "/275399/ishares-interest-rate-hedged-emerging-markets-bond-etf/1467271812596.ajax",
        "emgf": "/272820/ishares-msci-emerging-multi-factor-etf/1467271812596.ajax",
        "emhy": "/239527/ishares-emerging-markets-high-yield-bond-etf/1467271812596.ajax",
        "emif": "/239735/ishares-emerging-markets-infrastructure-etf/1467271812596.ajax",
        "emxc": "/288504/fund/1467271812596.ajax",
        "emxf": "/316020/fund/1467271812596.ajax",
        "enor": "/239673/ishares-msci-norway-capped-etf/1467271812596.ajax",
        "enzl": "/239672/ishares-msci-new-zealand-capped-etf/1467271812596.ajax",
        "ephe": "/239675/ishares-msci-philippines-etf/1467271812596.ajax",
        "epol": "/239676/ishares-msci-poland-capped-etf/1467271812596.ajax",
        "epp": "/239674/ishares-msci-pacific-ex-japan-etf/1467271812596.ajax",
        "epu": "/239606/ishares-msci-all-peru-capped-etf/1467271812596.ajax",
        "erus": "/239677/ishares-msci-russia-capped-etf/1467271812596.ajax",
        "esgd": "/283778/fund/1467271812596.ajax",
        "esge": "/283777/fund/1467271812596.ajax",
        "esgu": "/286007/fund/1467271812596.ajax",
        "esml": "/296644/fund/1467271812596.ajax",
        "eufn": "/239645/ishares-msci-europe-financials-etf/1467271812596.ajax",
        "eusa": "/239693/ishares-msci-usa-etf/1467271812596.ajax",
        "eusb": "/314499/fund/1467271812596.ajax",
        "ewa": "/239607/ishares-msci-australia-etf/1467271812596.ajax",
        "ewc": "/239615/ishares-msci-canada-etf/1467271812596.ajax",
        "ewd": "/239684/ishares-msci-sweden-etf/1467271812596.ajax",
        "ewg": "/239650/ishares-msci-germany-etf/1467271812596.ajax",
        "ewgs": "/239651/ishares-msci-germany-smallcap-etf/1467271812596.ajax",
        "ewh": "/239657/ishares-msci-hong-kong-etf/1467271812596.ajax",
        "ewi": "/239664/ishares-msci-italy-capped-etf/1467271812596.ajax",
        "ewj": "/239665/ishares-msci-japan-etf/1467271812596.ajax",
        "ewje": "/307265/fund/1467271812596.ajax",
        "ewjv": "/307263/fund/1467271812596.ajax",
        "ewk": "/239610/ishares-msci-belgium-capped-etf/1467271812596.ajax",
        "ewl": "/239685/ishares-msci-switzerland-capped-etf/1467271812596.ajax",
        "ewm": "/239669/ishares-msci-malaysia-etf/1467271812596.ajax",
        "ewn": "/239671/ishares-msci-netherlands-etf/1467271812596.ajax",
        "ewo": "/239609/ishares-msci-austria-capped-etf/1467271812596.ajax",
        "ewp": "/239683/ishares-msci-spain-capped-etf/1467271812596.ajax",
        "ewq": "/239648/ishares-msci-france-etf/1467271812596.ajax",
        "ews": "/239678/ishares-msci-singapore-capped-etf/1467271812596.ajax",
        "ewt": "/239686/ishares-msci-taiwan-etf/1467271812596.ajax",
        "ewu": "/239690/ishares-msci-united-kingdom-etf/1467271812596.ajax",
        "ewus": "/239691/ishares-msci-united-kingdom-smallcap-etf/1467271812596.ajax",
        "eww": "/239670/ishares-msci-mexico-capped-etf/1467271812596.ajax",
        "ewy": "/239681/ishares-msci-south-korea-capped-etf/1467271812596.ajax",
        "ewz": "/239612/ishares-msci-brazil-capped-etf/1467271812596.ajax",
        "ewzs": "/239613/ishares-msci-brazil-smallcap-etf/1467271812596.ajax",
        "exi": "/239745/ishares-global-industrials-etf/1467271812596.ajax",
        "eza": "/239680/ishares-msci-south-africa-etf/1467271812596.ajax",
        "ezu": "/239644/ishares-msci-emu-etf/1467271812596.ajax",
        "faln": "/283855/fund/1467271812596.ajax",
        "fibr": "/271544/ishares-us-fixed-income-balanced-risk-etf/1467271812596.ajax",
        "fill": "/239653/ishares-msci-global-energy-producers-etf/1467271812596.ajax",
        "flot": "/239534/ishares-floating-rate-bond-etf/1467271812596.ajax",
        "fm": "/239649/ishares-msci-frontier-100-etf/1467271812596.ajax",
        "fovl": "/307330/fund/1467271812596.ajax",
        "fxi": "/239536/ishares-china-largecap-etf/1467271812596.ajax",
        "gbf": "/239462/ishares-governmentcredit-bond-etf/1467271812596.ajax",
        "ghyg": "/239551/ishares-global-high-yield-corporate-bond-etf/1467271812596.ajax",
        "gnma": "/239461/ishares-gnma-bond-etf/1467271812596.ajax",
        "govt": "/239468/ishares-us-treasury-bond-etf/1467271812596.ajax",
        "govz": "/315911/fund/1467271812596.ajax",
        "gsg": "/239757/ishares-sp-gsci-commodityindexed-trust-fund/1467271812596.ajax",
        "gvi": "/239464/ishares-intermediate-governmentcredit-bond-etf/1467271812596.ajax",
        "hawx": "/273768/ishares-currency-hedged-msci-acwi-ex-us-etf/1467271812596.ajax",
        "hdv": "/239563/ishares-high-dividend-etf/1467271812596.ajax",
        "heem": "/268704/ishares-currency-hedged-msci-emerging-markets/1467271812596.ajax",
        "hefa": "/259622/ishares-currency-hedged-msci-eafe-etf/1467271812596.ajax",
        "hewc": "/273775/ishares-currency-hedged-msci-canada-etf/1467271812596.ajax",
        "hewg": "/259623/ishares-currency-hedged-msci-germany-etf/1467271812596.ajax",
        "hewj": "/259624/ishares-currency-hedged-msci-japan-etf/1467271812596.ajax",
        "hewu": "/273763/ishares-currency-hedged-msci-united-kingdom-etf/1467271812596.ajax",
        "heww": "/273748/ishares-currency-hedged-msci-mexico-etf/1467271812596.ajax",
        "hezu": "/268708/ishares-currency-hedged-msci-emu-etf/1467271812596.ajax",
        "hjpx": "/279286/ishares-currency-hedged-jpx-nikkei-400-etf/1467271812596.ajax",
        "hscz": "/273771/ishares-currency-hedged-msci-eafe-small-cap-etf/1467271812596.ajax",
        "hybb": "/305259/fund/1467271812596.ajax",
        "hydb": "/288478/fund/1467271812596.ajax",
        "hyg": "/239565/ishares-iboxx-high-yield-corporate-bond-etf/1467271812596.ajax",
        "hygh": "/264544/ishares-interest-rate-hedged-high-yield-bond-etf/1467271812596.ajax",
        "hyxf": "/283857/fund/1467271812596.ajax",
        "hyxu": "/239550/ishares-global-ex-usd-high-yield-corporate-bond-etf/1467271812596.ajax",
        "iagg": "/279626/ishares-international-aggregate-bond-etf/1467271812596.ajax",
        "iai": "/239504/ishares-us-brokerdealers-etf/1467271812596.ajax",
        "iak": "/239515/ishares-us-insurance-etf/1467271812596.ajax",
        "iat": "/239521/ishares-us-regional-banks-etf/1467271812596.ajax",
        "iauf": "/293782/fund/1467271812596.ajax",
        "ibb": "/239699/ishares-nasdaq-biotechnology-etf/1467271812596.ajax",
        "ibce": "/251477/isharesbond-mar-2023-corporate-exfinancials-term-etf/1467271812596.ajax",
        "ibdd": "/254555/isharesbond-mar-2023-corporate-term-etf/1467271812596.ajax",
        "ibdm": "/272342/ishares-ibonds-dec-2021-corporate-etf/1467271812596.ajax",
        "ibdn": "/272343/ishares-ibonds-dec-2022-corporate-etf/1467271812596.ajax",
        "ibdo": "/272344/ishares-ibonds-dec-2023-corporate-etf/1467271812596.ajax",
        "ibdp": "/272345/ishares-ibonds-dec-2024-corporate-etf/1467271812596.ajax",
        "ibdq": "/272346/ishares-ibonds-dec-2025-corporate-etf/1467271812596.ajax",
        "ibdr": "/285027/ishares-ibonds-dec-2026-term-corporate-etf-fund/1467271812596.ajax",
        "ibds": "/290315/fund/1467271812596.ajax",
        "ibdt": "/304570/fund/1467271812596.ajax",
        "ibdu": "/310035/fund/1467271812596.ajax",
        "ibdv": "/314496/fund/1467271812596.ajax",
        "ibha": "/308559/fund/1467271812596.ajax",
        "ibhb": "/308561/fund/1467271812596.ajax",
        "ibhc": "/308563/fund/1467271812596.ajax",
        "ibhd": "/308565/fund/1467271812596.ajax",
        "ibhe": "/308567/fund/1467271812596.ajax",
        "ibhf": "/316512/fund/1467271812596.ajax",
        "ibmj": "/276546/ishares-ibonds-dec-2021-amt-free-muni-bond-etf/1467271812596.ajax",
        "ibmk": "/276544/ishares-ibonds-dec-2022-amt-free-muni-bond-etf/1467271812596.ajax",
        "ibml": "/287194/ishares-ibonds-dec-2023-term-muni-bond-etf-fund/1467271812596.ajax",
        "ibmm": "/282961/ishares-ibonds-dec-2024-term-muni-bond-etf/1467271812596.ajax",
        "ibmn": "/282964/ishares-ibonds-dec-2025-term-muni-bond-etf/1467271812596.ajax",
        "ibmo": "/308047/fund/1467271812596.ajax",
        "ibmp": "/308049/fund/1467271812596.ajax",
        "ibmq": "/308051/fund/1467271812596.ajax",
        "ibta": "/312431/fund/1467271812596.ajax",
        "ibtb": "/312440/fund/1467271812596.ajax",
        "ibtd": "/312444/fund/1467271812596.ajax",
        "ibte": "/312451/fund/1467271812596.ajax",
        "ibtf": "/312454/fund/1467271812596.ajax",
        "ibtg": "/312457/fund/1467271812596.ajax",
        "ibth": "/312460/fund/1467271812596.ajax",
        "ibti": "/312463/fund/1467271812596.ajax",
        "ibtj": "/312466/fund/1467271812596.ajax",
        "ibtk": "/314830/fund/1467271812596.ajax",
        "icf": "/239482/ishares-cohen-steers-reit-etf/1467271812596.ajax",
        "icln": "/239738/ishares-global-clean-energy-etf/1467271812596.ajax",
        "icol": "/254562/ishares-msci-colombia-capped-etf/1467271812596.ajax",
        "icsh": "/258806/ishares-liquidity-income-etf/1467271812596.ajax",
        "icvt": "/272819/ishares-convertible-bond-etf/1467271812596.ajax",
        "idev": "/286762/fund/1467271812596.ajax",
        "idna": "/308878/fund/1467271812596.ajax",
        "idrv": "/307332/fund/1467271812596.ajax",
        "idu": "/239524/ishares-us-utilities-etf/1467271812596.ajax",
        "idv": "/239499/ishares-international-select-dividend-etf/1467271812596.ajax",
        "iecs": "/292415/fund/1467271812596.ajax",
        "iedi": "/292414/fund/1467271812596.ajax",
        "ief": "/239456/ishares-710-year-treasury-bond-etf/1467271812596.ajax",
        "iefa": "/244049/ishares-core-msci-eafe-etf/1467271812596.ajax",
        "iefn": "/292421/fund/1467271812596.ajax",
        "iehs": "/292422/fund/1467271812596.ajax",
        "iei": "/239455/ishares-37-year-treasury-bond-etf/1467271812596.ajax",
        "ieih": "/292423/fund/1467271812596.ajax",
        "ieme": "/292424/fund/1467271812596.ajax",
        "iemg": "/244050/ishares-core-msci-emerging-markets-etf/1467271812596.ajax",
        "ieo": "/239517/ishares-us-oil-gas-exploration-production-etf/1467271812596.ajax",
        "ietc": "/292425/fund/1467271812596.ajax",
        "ieur": "/264617/ishares-core-msci-europe-etf/1467271812596.ajax",
        "ieus": "/239537/ishares-developed-smallcap-ex-north-america-etf/1467271812596.ajax",
        "iev": "/239736/ishares-europe-etf/1467271812596.ajax",
        "iez": "/239518/ishares-us-oil-equipment-services-etf/1467271812596.ajax",
        "ifgl": "/239540/ishares-international-developed-real-estate-etf/1467271812596.ajax",
        "ifra": "/294315/fund/1467271812596.ajax",
        "igbh": "/275397/ishares-interest-rate-hedged-10-year-credit-bond-etf/1467271812596.ajax",
        "ige": "/239768/ishares-north-american-natural-resources-etf/1467271812596.ajax",
        "igeb": "/288302/fund/1467271812596.ajax",
        "igf": "/239746/ishares-global-infrastructure-etf/1467271812596.ajax",
        "igib": "/239463/ishares-intermediate-credit-bond-etf/1467271812596.ajax",
        "iglb": "/239423/ishares-10-year-credit-bond-etf/1467271812596.ajax",
        "igm": "/239769/ishares-north-american-tech-etf/1467271812596.ajax",
        "ign": "/239770/ishares-north-american-techmultimedia-networking-etf/1467271812596.ajax",
        "igov": "/239830/ishares-international-treasury-bond-etf/1467271812596.ajax",
        "igro": "/283737/fund/1467271812596.ajax",
        "igsb": "/239451/ishares-13-year-credit-bond-etf/1467271812596.ajax",
        "igv": "/239771/ishares-north-american-techsoftware-etf/1467271812596.ajax",
        "ihak": "/307352/fund/1467271812596.ajax",
        "ihe": "/239519/ishares-us-pharmaceuticals-etf/1467271812596.ajax",
        "ihf": "/239510/ishares-us-healthcare-providers-etf/1467271812596.ajax",
        "ihi": "/239516/ishares-us-medical-devices-etf/1467271812596.ajax",
        "ijh": "/239763/ishares-core-sp-midcap-etf/1467271812596.ajax",
        "ijj": "/239764/ishares-sp-midcap-400-value-etf/1467271812596.ajax",
        "ijk": "/239762/ishares-sp-midcap-400-growth-etf/1467271812596.ajax",
        "ijr": "/239774/ishares-core-sp-smallcap-etf/1467271812596.ajax",
        "ijs": "/239775/ishares-sp-smallcap-600-value-etf/1467271812596.ajax",
        "ijt": "/239773/ishares-sp-smallcap-600-growth-etf/1467271812596.ajax",
        "ilf": "/239761/ishares-latin-america-40-etf/1467271812596.ajax",
        "iltb": "/239424/ishares-core-longterm-us-bond-etf/1467271812596.ajax",
        "imtb": "/285539/fund/1467271812596.ajax",
        "imtm": "/271538/ishares-msci-international-developed-momentum-factor-etf/1467271812596.ajax",
        "inda": "/239659/ishares-msci-india-etf/1467271812596.ajax",
        "indy": "/239758/ishares-india-50-etf/1467271812596.ajax",
        "intf": "/272822/ishares-msci-international-multi-factor-etf/1467271812596.ajax",
        "ioo": "/239737/ishares-global-100-etf/1467271812596.ajax",
        "ipac": "/264619/ishares-core-msci-pacific-etf/1467271812596.ajax",
        "ipff": "/239759/ishares-international-preferred-stock-etf/1467271812596.ajax",
        "iqlt": "/271540/ishares-msci-international-developed-quality-factor-etf/1467271812596.ajax",
        "irbo": "/297905/fund/1467271812596.ajax",
        "iscf": "/272823/ishares-msci-international-small-cap-multi-factor-etf/1467271812596.ajax",
        "ishg": "/239829/ishares-13-year-international-treasury-bond-etf/1467271812596.ajax",
        "istb": "/244051/ishares-core-shortterm-us-bond-etf/1467271812596.ajax",
        "isze": "/275384/ishares-msci-international-developed-size-factor-etf/1467271812596.ajax",
        "ita": "/239502/ishares-us-aerospace-defense-etf/1467271812596.ajax",
        "itb": "/239512/ishares-us-home-construction-etf/1467271812596.ajax",
        "itot": "/239724/ishares-core-sp-total-us-stock-market-etf/1467271812596.ajax",
        "iusb": "/264615/ishares-core-total-usd-bond-market-etf/1467271812596.ajax",
        "iusg": "/239713/ishares-core-sp-us-growth-etf/1467271812596.ajax",
        "iusv": "/239715/ishares-core-sp-us-value-etf/1467271812596.ajax",
        "ive": "/239728/ishares-sp-500-value-etf/1467271812596.ajax",
        "ivlu": "/275382/ishares-msci-international-developed-value-factor-etf/1467271812596.ajax",
        "ivv": "/239726/ishares-core-sp-500-etf/1467271812596.ajax",
        "ivw": "/239725/ishares-sp-500-growth-etf/1467271812596.ajax",
        "iwb": "/239707/ishares-russell-1000-etf/1467271812596.ajax",
        "iwc": "/239716/ishares-microcap-etf/1467271812596.ajax",
        "iwd": "/239708/ishares-russell-1000-value-etf/1467271812596.ajax",
        "iwf": "/239706/ishares-russell-1000-growth-etf/1467271812596.ajax",
        "iwfh": "/315979/fund/1467271812596.ajax",
        "iwl": "/239721/ishares-russell-top-200-etf/1467271812596.ajax",
        "iwm": "/239710/ishares-russell-2000-etf/1467271812596.ajax",
        "iwn": "/239712/ishares-russell-2000-value-etf/1467271812596.ajax",
        "iwo": "/239709/ishares-russell-2000-growth-etf/1467271812596.ajax",
        "iwp": "/239717/ishares-russell-midcap-growth-etf/1467271812596.ajax",
        "iwr": "/239718/ishares-russell-midcap-etf/1467271812596.ajax",
        "iws": "/239719/ishares-russell-midcap-value-etf/1467271812596.ajax",
        "iwv": "/239714/ishares-russell-3000-etf/1467271812596.ajax",
        "iwx": "/239722/ishares-russell-top-200-value-etf/1467271812596.ajax",
        "iwy": "/239720/ishares-russell-top-200-growth-etf/1467271812596.ajax",
        "ixc": "/239741/ishares-global-energy-etf/1467271812596.ajax",
        "ixg": "/239742/ishares-global-financials-etf/1467271812596.ajax",
        "ixj": "/239744/ishares-global-healthcare-etf/1467271812596.ajax",
        "ixn": "/239750/ishares-global-tech-etf/1467271812596.ajax",
        "ixp": "/239751/ishares-global-telecom-etf/1467271812596.ajax",
        "ixus": "/244048/ishares-core-msci-total-international-stock-etf/1467271812596.ajax",
        "iyc": "/239506/ishares-us-consumer-services-etf/1467271812596.ajax",
        "iye": "/239507/ishares-us-energy-etf/1467271812596.ajax",
        "iyf": "/239508/ishares-us-financials-etf/1467271812596.ajax",
        "iyg": "/239509/ishares-us-financial-services-etf/1467271812596.ajax",
        "iyh": "/239511/ishares-us-healthcare-etf/1467271812596.ajax",
        "iyj": "/239514/ishares-us-industrials-etf/1467271812596.ajax",
        "iyk": "/239505/ishares-us-consumer-goods-etf/1467271812596.ajax",
        "iyld": "/239585/ishares-morningstar-multiasset-income-etf/1467271812596.ajax",
        "iym": "/239503/ishares-us-basic-materials-etf/1467271812596.ajax",
        "iyr": "/239520/ishares-us-real-estate-etf/1467271812596.ajax",
        "iyt": "/239501/ishares-transportation-average-etf/1467271812596.ajax",
        "iyw": "/239522/ishares-us-technology-etf/1467271812596.ajax",
        "iyy": "/239513/ishares-dow-jones-us-etf/1467271812596.ajax",
        "iyz": "/239523/ishares-us-telecommunications-etf/1467271812596.ajax",
        "jkd": "/239579/ishares-morningstar-largecap-etf/1467271812596.ajax",
        "jke": "/239580/ishares-morningstar-largecap-growth-etf/1467271812596.ajax",
        "jkf": "/239581/ishares-morningstar-largecap-value-etf/1467271812596.ajax",
        "jkg": "/239582/ishares-morningstar-midcap-etf/1467271812596.ajax",
        "jkh": "/239583/ishares-morningstar-midcap-growth-etf/1467271812596.ajax",
        "jki": "/239584/ishares-morningstar-midcap-value-etf/1467271812596.ajax",
        "jkj": "/239586/ishares-morningstar-smallcap-etf/1467271812596.ajax",
        "jkk": "/239587/ishares-morningstar-smallcap-growth-etf/1467271812596.ajax",
        "jkl": "/239588/ishares-morningstar-smallcap-value-etf/1467271812596.ajax",
        "jpxn": "/239831/ishares-japan-largecap-etf/1467271812596.ajax",
        "jxi": "/239753/ishares-global-utilities-etf/1467271812596.ajax",
        "ksa": "/271542/ishares-msci-saudi-arabia-capped-etf/1467271812596.ajax",
        "kwt": "/312763/fund/1467271812596.ajax",
        "kxi": "/239740/ishares-global-consumer-staples-etf/1467271812596.ajax",
        "ldem": "/312222/fund/1467271812596.ajax",
        "lemb": "/239528/ishares-emerging-markets-local-currency-bond-etf/1467271812596.ajax",
        "lqd": "/239566/ishares-iboxx-investment-grade-corporate-bond-etf/1467271812596.ajax",
        "lqdh": "/264542/ishares-interest-rate-hedged-corporate-bond-etf/1467271812596.ajax",
        "lqdi": "/294319/fund/1467271812596.ajax",
        "lrgf": "/272824/ishares-msci-usa-multi-factor-etf/1467271812596.ajax",
        "mbb": "/239465/ishares-mbs-etf/1467271812596.ajax",
        "mchi": "/239619/ishares-msci-china-etf/1467271812596.ajax",
        "mear": "/272112/ishares-short-maturity-municipal-bond-etf/1467271812596.ajax",
        "midf": "/308876/fund/1467271812596.ajax",
        "mtum": "/251614/ishares-msci-usa-momentum-factor-etf/1467271812596.ajax",
        "mub": "/239766/ishares-national-amtfree-muni-bond-etf/1467271812596.ajax",
        "mxi": "/239748/ishares-global-materials-etf/1467271812596.ajax",
        "near": "/239854/ishares-short-maturity-bond-etf/1467271812596.ajax",
        "nyf": "/239767/ishares-new-york-amtfree-muni-bond-etf/1467271812596.ajax",
        "oef": "/239723/ishares-sp-100-etf/1467271812596.ajax",
        "pff": "/239826/ishares-us-preferred-stock-etf/1467271812596.ajax",
        "pick": "/239655/ishares-msci-global-metals-mining-producers-etf/1467271812596.ajax",
        "qat": "/264273/ishares-msci-qatar-capped-etf/1467271812596.ajax",
        "qlta": "/239431/ishares-aaa-a-rated-corporate-bond-etf/1467271812596.ajax",
        "qual": "/256101/ishares-msci-usa-quality-factor-etf/1467271812596.ajax",
        "reet": "/268752/ishares-global-reit-etf/1467271812596.ajax",
        "rem": "/239543/ishares-mortgage-real-estate-capped-etf/1467271812596.ajax",
        "rez": "/239545/ishares-residential-real-estate-capped-etf/1467271812596.ajax",
        "ring": "/239654/ishares-msci-global-gold-miners-etf/1467271812596.ajax",
        "rxi": "/239739/ishares-global-consumer-discretionary-etf/1467271812596.ajax",
        "scj": "/239666/ishares-msci-japan-smallcap-etf/1467271812596.ajax",
        "scz": "/239627/ishares-msci-eafe-smallcap-etf/1467271812596.ajax",
        "sdg": "/283378/fund/1467271812596.ajax",
        "sgov": "/314116/fund/1467271812596.ajax",
        "shv": "/239466/ishares-short-treasury-bond-etf/1467271812596.ajax",
        "shy": "/239452/ishares-13-year-treasury-bond-etf/1467271812596.ajax",
        "shyg": "/258100/ishares-05-year-high-yield-corporate-bond-etf/1467271812596.ajax",
        "size": "/251465/ishares-msci-usa-size-factor-etf/1467271812596.ajax",
        "slqd": "/258098/ishares-05-year-investment-grade-corporate-bond-etf/1467271812596.ajax",
        "slvp": "/239656/ishares-msci-global-silver-miners-etf/1467271812596.ajax",
        "smin": "/239660/ishares-msci-india-smallcap-etf/1467271812596.ajax",
        "smlf": "/272825/ishares-msci-usa-small-cap-multi-factor-etf/1467271812596.ajax",
        "smmd": "/288024/fund/1467271812596.ajax",
        "smmv": "/284609/fund/1467271812596.ajax",
        "soxx": "/239705/ishares-phlx-semiconductor-etf/1467271812596.ajax",
        "stip": "/239450/ishares-05-year-tips-bond-etf/1467271812596.ajax",
        "stlc": "/313074/fund/1467271812596.ajax",
        "stlg": "/312212/fund/1467271812596.ajax",
        "stlv": "/312214/fund/1467271812596.ajax",
        "stmb": "/313096/fund/1467271812596.ajax",
        "stsb": "/313101/fund/1467271812596.ajax",
        "sub": "/239772/ishares-shortterm-national-amtfree-muni-bond-etf/1467271812596.ajax",
        "susa": "/239692/ishares-msci-usa-esg-select-etf/1467271812596.ajax",
        "susb": "/288490/fund/1467271812596.ajax",
        "susc": "/288488/fund/1467271812596.ajax",
        "susl": "/308574/fund/1467271812596.ajax",
        "sval": "/316394/fund/1467271812596.ajax",
        "tecb": "/312046/fund/1467271812596.ajax",
        "tflo": "/260652/ishares-treasury-floating-rate-bond-etf/1467271812596.ajax",
        "thd": "/239688/ishares-msci-thailand-capped-etf/1467271812596.ajax",
        "tip": "/239467/ishares-tips-bond-etf/1467271812596.ajax",
        "tlh": "/239453/ishares-1020-year-treasury-bond-etf/1467271812596.ajax",
        "tlt": "/239454/ishares-20-year-treasury-bond-etf/1467271812596.ajax",
        "tok": "/239668/ishares-msci-kokusai-etf/1467271812596.ajax",
        "tur": "/239689/ishares-msci-turkey-etf/1467271812596.ajax",
        "uae": "/264275/ishares-msci-uae-capped-etf/1467271812596.ajax",
        "urth": "/239696/ishares-msci-world-etf/1467271812596.ajax",
        "ushy": "/291299/fund/1467271812596.ajax",
        "usig": "/239460/ishares-credit-bond-etf/1467271812596.ajax",
        "usmv": "/239695/ishares-msci-usa-minimum-volatility-etf/1467271812596.ajax",
        "usrt": "/239544/ishares-real-estate-50-etf/1467271812596.ajax",
        "usxf": "/314365/fund/1467271812596.ajax",
        "vegi": "/239652/ishares-msci-global-agriculture-producers-etf/1467271812596.ajax",
        "vlue": "/251616/ishares-msci-usa-value-factor-etf/1467271812596.ajax",
        "wood": "/239752/ishares-global-timber-forestry-etf/1467271812596.ajax",
        "wps": "/239734/ishares-international-developed-property-etf/1467271812596.ajax",
        "xjh": "/315914/fund/1467271812596.ajax",
        "xjr": "/315920/fund/1467271812596.ajax",
        "xt": "/272532/ishares-exponential-technologies-etf/1467271812596.ajax",
        "xvv": "/315917/fund/1467271812596.ajax",
    }
    return f"https://www.ishares.com/us/products{funds_basepaths[symbol]}?fileType=csv&fileName={symbol.upper()}_holdings&dataType=fund"

def fetch(  fund: str, 
            headers: Mapping[str,str] = None) -> Union[None, List[Tuple[str,float]]]:

    global FUNDS
    assert fund in FUNDS
    if headers is None:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36"
        }
    fund_csv_url = get_fund_file(fund)
    req = urllib.request.Request(
        fund_csv_url, 
        headers=headers
    )
    res = urllib.request.urlopen(req)
    data = csv.reader([l.decode("utf-8").strip() for l in res.readlines()])
    for i in range(0, 10):
        next(data)
    result: Mapping[str, float] = dict()
    for holding in data:
        try:
            ticker = holding[0]
            weight = holding[5]
            asset_class = holding[3]
            if not ticker or not weight or not (asset_class == "Equity"):
                continue
            result[ticker] = result.get(ticker, 0) + float(weight)
        except IndexError:
            break
    return [(holding, weight) for holding, weight in result.items()]