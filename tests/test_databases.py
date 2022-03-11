# test_dbms.py 

# standard library dependencies

# external dependencies
import pytest

# local dependencies
from src.backend import select_database

test_etfs = [
    ("SPY", True),
    ("ARKK", True),
    ("BKLN", True),
    ("aaxj", True),
    ("BULL", False),
]
@pytest.mark.parametrize("etf,answer", test_etfs)
def test_zack_scraper(etf, answer):
    for db in ('sqlite3','tinydb'):
        db_client = select_database(db)
        if answer is False:
            with pytest.raises(Exception):
                db_client.get_holdings_and_weights_for_etf(etf)
        else:
            assert len(db_client.get_holdings_and_weights_for_etf(etf)) > 0
