"""
Import term type collectors into parent package.
"""

from .dataverse import DataverseCollector
from .figshare import FigshareCollector
from .papers_with_code import PapersWithCodeCollector

try:
    from .kaggle import KaggleCollector
except (OSError, ModuleNotFoundError):
    pass
