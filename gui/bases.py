import threading
import tkinter as tk
from scrapers import DataverseScraper, DryadScraper, FigshareScraper, \
    KaggleScraper, OpenMLScraper, PapersWithCodeScraper, UCIScraper, \
    ZenodoScraper

# Variables for scraper selection
idx_to_repo_selection_dict = {
    0: 'Dataverse',
    1: 'Dryad',
    2: 'Figshare',
    3: 'Kaggle',
    4: 'OpenML',
    5: 'Papers With Code',
    6: 'UCI',
    7: 'Zenodo'
}

repo_name_to_class_dict = {
    'Dataverse': DataverseScraper,
    'Dryad': DryadScraper,
    'Figshare': FigshareScraper,
    'Kaggle': KaggleScraper,
    'OpenML': OpenMLScraper,
    'Papers With Code': PapersWithCodeScraper,
    'UCI': UCIScraper,
    'Zenodo': ZenodoScraper
}


class ThreadedRun(threading.Thread):
    def __init__(self, scraper, **kwargs):
        self.scraper = scraper
        super().__init__(target=self.scraper.run, **kwargs)


class Page(tk.Frame):
    def __init__(self, *args, **kwargs):
        tk.Frame.__init__(self, *args, **kwargs)

    def show(self):
        self.tkraise()
