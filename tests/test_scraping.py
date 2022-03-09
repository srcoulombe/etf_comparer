# test_scraping.py 

# standard library dependencies

# external dependencies
import pytest

# local dependencies
from src.scraping.zack_scraper import fetch as zack_scraper
from src.scraping.ark_scraper import fetch as ark_scraper
from src.scraping.invesco_scraper import fetch as invesco_scraper
from src.scraping.ishares_scraper import fetch as ishares_scraper

zack_scraping_tests = [
    ("", False),
    ("NONSENSE_ETF", False),
    ("QQQ", True)
]
@pytest.mark.parametrize("case,answer", zack_scraping_tests)
def test_zack_scraper(case, answer):
    assert (len(zack_scraper(case)) > 0) == answer

ark_scraping_tests = [
    ("", False),
    ("Nonsense", False),
    ("ARKK", True),
    ('ARKW', True),
    ('ARKQ', True), 
    ('ARKF', True),
    ('ARKG', True)
]
@pytest.mark.parametrize("case,answer", ark_scraping_tests)
def test_ark_scraper(case, answer):
    if answer is False:
        with pytest.raises(AssertionError):
            ark_scraper(case)
    else:
        assert (len(ark_scraper(case)) > 0) == answer   

invesco_scraping_tests = [
    ("BKLN", True),
    ("ADRE", True),
    ("", False),
    ("Nonsense", False)
]
@pytest.mark.parametrize("case,answer", invesco_scraping_tests)
def test_invesco_scraper(case, answer):
    if answer is False:
        with pytest.raises(AssertionError):
            invesco_scraper(case)
    else:
        assert (len(invesco_scraper(case)) > 0) == answer   

ishares_scraping_tests = [
    ('aaxj', True),
    ('acwf', True),
    ('', False),
    ('Nonsense', False)
]
@pytest.mark.parametrize("case,answer", ishares_scraping_tests)
def test_ishares_scraper(case, answer):
    if answer is False:
        with pytest.raises(AssertionError):
            ishares_scraper(case)
    else:
        assert (len(ishares_scraper(case)) > 0) == answer   
