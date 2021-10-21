from .base_scrapers import AbstractScraper, AbstractAPIScraper, AbstractWebScraper
from .term_scrapers import AbstractTermScraper, DryadScraper, ZenodoScraper
from .type_scrapers import AbstractTypeScraper, OpenMLScraper
from .term_type_scrapers import (AbstractTermTypeScraper, DataverseScraper, 
    FigshareScraper, KaggleScraper, PapersWithCodeScraper)
from .web_scrapers import UCIScraper