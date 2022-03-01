from .Dataverse import DataverseScraper
from .Figshare import FigshareScraper
from .PapersWithCode import PapersWithCodeScraper

# Try to load Kaggle
try:
    from .Kaggle import KaggleScraper
except OSError:
    pass
