from abc import abstractmethod
import threading
import tkinter.ttk as ttk
from pycurator.scrapers import DataverseScraper, DryadScraper, FigshareScraper, \
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


class ViewPage(ttk.Frame):
    def __init__(self, *args, **kwargs):
        ttk.Frame.__init__(self, *args, **kwargs)

        self.controller = None
        self.is_initialized = False
        self.next_page_button = None

    def set_controller(self, controller):
        self.controller = controller

    @staticmethod
    def no_overwrite(show_func):
        def display(self):
            raised = self.attempt_raise()
            if not raised:
                show_func(self)
                self.is_initialized = True

        return display

    @abstractmethod
    def reset_frame(self):
        raise NotImplementedError

    def attempt_raise(self):
        if self.is_initialized:
            try:
                self.reset_frame()
            except AttributeError:
                pass

            self.tkraise()
            return True
        return False
