"""
Import term type collectors into parent package.
"""

from .Dataverse import DataverseCollector
from .Figshare import FigshareCollector
from .PapersWithCode import PapersWithCodeCollector

try:
    from .Kaggle import KaggleCollector
except (OSError, ModuleNotFoundError):
    pass
