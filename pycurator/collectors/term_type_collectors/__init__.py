from .Dataverse import DataverseCollector
from .Figshare import FigshareCollector
from .PapersWithCode import PapersWithCodeCollector

# Try to load Kaggle
try:
    from .Kaggle import KaggleCollector
except (OSError, ModuleNotFoundError):
    pass
