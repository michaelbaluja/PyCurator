from .base import (
    BaseCollector,
    BaseAPICollector,
    BaseWebCollector,
    BaseTermCollector,
    BaseTypeCollector,
    BaseTermTypeCollector,
    TermQueryMixin,
    TypeQueryMixin,
    WebPathScraperMixin
)
from .term_collectors import DryadCollector, ZenodoCollector
from .term_type_collectors import (
    DataverseCollector,
    FigshareCollector,
    PapersWithCodeCollector
)
from .type_collectors import OpenMLCollector

try:
    from .term_type_collectors import KaggleCollector
except ImportError:
    KaggleScraper = None

from .web_scrapers import UCIScraper

# Create dict of collectors available & sort
available_repos = {
    'Dryad': DryadCollector,
    'Zenodo': ZenodoCollector,
    'OpenML': OpenMLCollector,
    'Dataverse': DataverseCollector,
    'Figshare': FigshareCollector,
    'Papers With Code': PapersWithCodeCollector,
    'UCI': UCIScraper
}

if KaggleCollector:
    available_repos['Kaggle'] = KaggleCollector

available_repos = dict(sorted(available_repos.items()))
