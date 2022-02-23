import os
import tkinter as tk
from .bases import ThreadedRun
from .landing_page import LandingPage
from .selection_page import SelectionPage
from .run_page import RunPage
from pycurator.scrapers import (
    TermScraperMixin,
    TypeScraperMixin,
    WebPathScraperMixin
)


class CuratorView(tk.Frame):
    def __init__(self, parent):
        super().__init__(parent)
        self.pages = dict()
        self.current_page = None

        self.grid_rowconfigure(0, weight=1)
        self.grid_columnconfigure(0, weight=1)
        self.grid(row=0, column=0, sticky='nsew')

        for F in (LandingPage, SelectionPage, RunPage):
            self.pages[F] = F(self)

    def set_controller(self, controller):
        for page in self.pages.values():
            page.set_controller(controller)

    def show(self, page=LandingPage):
        self.current_page = page
        self.pages[page].show()


class ScraperModel:
    def __init__(self, scraper_class, scraper_name):
        self.scraper_class = scraper_class
        self.scraper_name = scraper_name
        self.scraper = None
        self.threaded_run = None

        self.requirements = {
            'search_terms': issubclass(self.scraper_class, TermScraperMixin),
            'search_types': issubclass(self.scraper_class, TypeScraperMixin),
            'path_file': issubclass(self.scraper_class, WebPathScraperMixin)
        }

    def initialize_scraper(self, param_val_kwargs):
        self.scraper = self.scraper_class(**param_val_kwargs)

    def initialize_thread(self, thread_kwargs):
        self.threaded_run = ThreadedRun(
            scraper=self.scraper,
            kwargs=thread_kwargs
        )


class CuratorController:
    def __init__(self, model, view):
        self.model = model
        self.view = view

        self.runtime_param_vars = dict()

    def request_next_page(self, *args):
        try:
            self.view.pages.get(
                self.view.current_page
            ).next_page_button.invoke()
        except AttributeError:
            raise UserWarning(
                '"<Return>" button not active for this page.'
            )

    def request_execution(self):
        self.model.scraper.request_execution()

    def show(self, page=None):
        self.view.show(page)

    def set_model(self, scraper, scraper_name):
        self.model = ScraperModel(scraper, scraper_name)
        self.runtime_requirements = self.model.requirements
        self.runtime_param_vars[scraper_name] = dict()

    def add_run_parameter(self, param, value):
        self.runtime_param_vars[self.model.scraper_name][param] = value

    def get_run_parameter(self, param):
        return self.runtime_param_vars[self.model.scraper_name][param]

    @property
    def runtime_requirements(self):
        return self._runtime_requirements

    @runtime_requirements.setter
    def runtime_requirements(self, requirements):
        self._runtime_requirements = requirements

    def evaluate_parameter(self, param_name):
        param_var = self.get_run_parameter(param_name)

        # Returned variable is list-like
        if hasattr(param_var, '__iter__'):
            return {
                variable: self.evaluate_parameter(variable)
                for variable in param_var
            }

        try:
            return param_var.get()
        except AttributeError:
            raise ValueError(
                f'Param "{param_name} not present in '
                f'"{self.model.scraper_name}".'
            )

    def parse_run_parameters(self):
        """Ensure necessary requirements are present before running.

        See Also
        --------
        scrapers.base_scrapers
        """

        param_val_kwargs = {
            param_name: self.evaluate_parameter(param_name)
            for param_name in self.runtime_param_vars[self.model.scraper_name]
        }

        # Add default save directory
        if 'save_dir' not in param_val_kwargs:
            param_val_kwargs['save_dir'] = os.path.join(
                'data',
                self.model.scraper_name
            )
            os.makedirs(param_val_kwargs['save_dir'], exist_ok=True)

        missing_reqs = [
            param_name
            for param_name, is_req in self.runtime_requirements.items()
            if is_req and not param_val_kwargs.get(param_name)
        ]

        if missing_reqs:
            self.view.alert_missing_reqs(missing_reqs)
        else:
            self.initialize_run(param_val_kwargs)

    def initialize_run(self, param_val_kwargs):
        save_dir = param_val_kwargs.pop('save_dir')

        # Set up the run thread
        self.model.initialize_scraper(param_val_kwargs)
        self.model.initialize_thread({'save_dir': save_dir})

        # Show run page
        self.view.show(RunPage)

    def run_scraper(self, update_func):
        self.model.threaded_run.start()
        self.view.after(100, update_func)
