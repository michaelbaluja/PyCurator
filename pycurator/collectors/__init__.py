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

available_repos = {
    'Dryad': DryadCollector,
    'Zenodo': ZenodoCollector,
    'Dataverse': DataverseCollector,
    'Figshare': FigshareCollector,
    'Papers With Code': PapersWithCodeCollector,
}

try:
    from .term_type_collectors import KaggleCollector
    available_repos['Kaggle'] = KaggleCollector
except ImportError:
    pass

try:
    from .type_collectors import OpenMLCollector
    available_repos['OpenML'] = OpenMLCollector
except ImportError:
    pass

available_repos = dict(sorted(available_repos.items()))
