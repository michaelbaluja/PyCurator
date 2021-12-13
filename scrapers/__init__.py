from .base_scrapers import AbstractScraper, AbstractAPIScraper, \
    AbstractWebScraper, AbstractTermScraper, AbstractTypeScraper, \
    AbstractTermTypeScraper
from .term_scrapers import DryadScraper, ZenodoScraper
from .type_scrapers import OpenMLScraper
from .term_type_scrapers import DataverseScraper, FigshareScraper, \
    KaggleScraper, PapersWithCodeScraper
from .web_scrapers import UCIScraper