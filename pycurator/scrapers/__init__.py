from .base_scrapers import (
    AbstractScraper,
    AbstractAPIScraper,
    AbstractWebScraper,
    AbstractTermScraper,
    AbstractTypeScraper,
    AbstractTermTypeScraper,
    TermScraperMixin,
    TypeScraperMixin,
    WebPathScraperMixin
)
from .term_scrapers import DryadScraper, ZenodoScraper
from .type_scrapers import OpenMLScraper
from .term_type_scrapers import (
    DataverseScraper,
    FigshareScraper,
    PapersWithCodeScraper
)

try:
    from .term_type_scrapers import KaggleScraper
except ImportError:
    KaggleScraper = None

from .web_scrapers import UCIScraper

# Create dict of scrapers available & sort
available_scrapers = {
    'Dryad': DryadScraper,
    'Zenodo': ZenodoScraper,
    'OpenML': OpenMLScraper,
    'Dataverse': DataverseScraper,
    'Figshare': FigshareScraper,
    'Papers With Code': PapersWithCodeScraper,
    'UCI': UCIScraper
}

if KaggleScraper:
    available_scrapers['Kaggle'] = KaggleScraper

available_scrapers = {
    name: scraper
    for name, scraper in sorted(available_scrapers.items())
}
