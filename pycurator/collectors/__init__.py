from .base import (
    BaseCollector,
    BaseAPICollector,
    BaseTermCollector,
    BaseTypeCollector,
    BaseTermTypeCollector,
    TermQueryMixin,
    TypeQueryMixin,
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

# Create dict of collectors available & sort
available_repos = {
    'Dryad': DryadCollector,
    'Zenodo': ZenodoCollector,
    'OpenML': OpenMLCollector,
    'Dataverse': DataverseCollector,
    'Figshare': FigshareCollector,
    'Papers With Code': PapersWithCodeCollector,
}

if KaggleCollector:
    available_repos['Kaggle'] = KaggleCollector

available_repos = dict(sorted(available_repos.items()))
