from scrapers import *

def test_get_repo_name():
    assert PapersWithCodeScraper.get_repo_name() == 'PapersWithCode'