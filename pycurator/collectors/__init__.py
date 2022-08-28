"""
Import collector format modules and create valid collector dict.
"""

from . import base, term_collectors, term_type_collectors, type_collectors

available_repos = {
    "Dryad": term_collectors.DryadCollector,
    "Zenodo": term_collectors.ZenodoCollector,
    "Dataverse": term_type_collectors.DataverseCollector,
    "Figshare": term_type_collectors.FigshareCollector,
    "Papers With Code": term_type_collectors.PapersWithCodeCollector,
}

try:
    from .term_type_collectors import KaggleCollector

    available_repos["Kaggle"] = KaggleCollector
except ImportError:
    pass

try:
    from .type_collectors import OpenMLCollector

    available_repos["OpenML"] = OpenMLCollector
except ImportError:
    pass

available_repos = dict(map(tuple, sorted(available_repos.items())))
