from __future__ import annotations
import _tkinter
import os
import tkinter as tk
import tkinter.ttk as ttk

from .bases import ThreadedRun, ViewPage
from .landing_page import LandingPage
from .selection_page import SelectionPage
from .run_page import RunPage
from pycurator.scrapers import (
    TermScraperMixin,
    TypeScraperMixin
)

from typing import Type, TypeVar, ParamSpec
from pycurator.scrapers import AbstractScraper
import queue


Page = TypeVar('Page', bound=ViewPage)
P = ParamSpec('P')


class CuratorView(ttk.Frame):
    def __init__(self, parent: tk.Tk) -> None:
        super().__init__(parent)
        self.pages = dict()
        self.current_page = None

        self.grid_rowconfigure(0, weight=1)
        self.grid_columnconfigure(0, weight=1)
        self.grid(row=0, column=0, sticky='nsew')

        for F in (LandingPage, SelectionPage, RunPage):
            self.pages[F] = F(self)

    def set_controller(self, controller: CuratorController) -> None:
        for page in self.pages.values():
            page.set_controller(controller)

    def show(self, page: Type[Page] = LandingPage) -> None:
        self.current_page = self.pages[page]
        self.current_page.show()


class ScraperModel:
    def __init__(
            self,
            scraper_class: Type[AbstractScraper],
            scraper_name: str
    ) -> None:
        self.scraper_class = scraper_class
        self.scraper_name = scraper_name
        self.scraper = None
        self.threaded_run = None

        self.requirements = {
            'search_terms': issubclass(self.scraper_class, TermScraperMixin),
            'search_types': issubclass(self.scraper_class, TypeScraperMixin)
        }

    def initialize_scraper(self, param_val_kwargs: P.kwargs) -> None:
        self.scraper = self.scraper_class(**param_val_kwargs)

    def initialize_thread(self, thread_kwargs: P.kwargs) -> None:
        self.threaded_run = ThreadedRun(
            scraper=self.scraper,
            kwargs=thread_kwargs
        )


class CuratorController:
    def __init__(self, model, view):
        self.model = model
        self.view = view

        self.runtime_param_vars = dict()

    def request_next_page(self, *args: P.args):
        try:
            self.view.current_page.next_page_button.invoke()
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

        if isinstance(param_var, dict):
            return [key for key, val in param_var.items() if val.get()]

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
            self.view.current_page.alert_missing_reqs(missing_reqs)
        else:
            self.initialize_run(param_val_kwargs)

    def initialize_run(self, param_val_kwargs: P.kwargs):
        save_dir = param_val_kwargs.pop('save_dir')

        # Set up the run thread
        self.model.initialize_scraper(param_val_kwargs)
        self.model.initialize_thread({'save_dir': save_dir})

        # Show run page
        self.view.show(RunPage)

    def run_scraper(self):
        self.model.threaded_run.start()
        self.view.after(100, self.process_runtime_updates)

    def process_runtime_updates(self) -> None:
        """Push status update from scraper queue to output."""
        # Check for updates to progress bar
        if self.model.threaded_run.scraper.num_queries is not None:
            self._update_progress_bar()

        # Get next object in queue and push to output widget.
        try:
            msg = self.model.threaded_run.scraper.queue.get_nowait()

            self._update_runtime_output(msg)

            # Check if process still running
            if self.model.scraper.continue_running:
                self.view.after(100, self.process_runtime_updates)
            else:
                # Empty queue
                while not self.model.threaded_run.scraper.queue.empty():
                    self.process_runtime_updates()

                # Stop progress bar
                self.view.current_page.progress_bar.stop()

                # Reactivate back button
                self.view.current_page.back_button.config(state='normal')

                # Replace Stop button with Exit button
                self.view.current_page.stop_button.pack_forget()
                self.view.current_page.exit_button.pack(side='left')

                return
        # If the queue is empty, continually check
        except queue.Empty:
            self.view.after(100, self.process_runtime_updates)

    def _update_progress_bar_indeterminate(self) -> None:
        self.view.current_page.progress_bar['mode'] = 'indeterminate'
        if self.model.threaded_run.scraper.num_queries:
            self.view.current_page.progress_bar.start()
        else:
            self.view.current_page.progress_bar.stop()
            self.view.current_page.progress_bar['value'] = 0

    def _update_progress_bar_determinate(self) -> None:
        self.view.current_page.progress_bar.stop()
        self.view.current_page.progress_determinate_num['text'] = \
            f'({self.model.threaded_run.scraper.queries_completed}/' \
            f'{self.model.threaded_run.scraper.num_queries})'
        self.view.current_page.progress_bar['mode'] = 'determinate'
        self.view.current_page.progress_bar['value'] = \
            (self.model.threaded_run.scraper.queries_completed /
                self.model.threaded_run.scraper.num_queries * 100)

    def _update_progress_bar(self) -> None:
        """Update status of progress bar based on scraper status."""
        self.view.current_page.progress_label['text'] = \
            self.model.threaded_run.scraper.current_query_ref

        if isinstance(self.model.threaded_run.scraper.num_queries, bool):
            self._update_progress_bar_indeterminate()
        else:
            self._update_progress_bar_determinate()

    def _update_runtime_output(
            self,
            update: str,
            loc: _tkinter.Tcl_Obj | str | float | tk.Misc = 'end',
            newline: bool = True
    ) -> None:
        if newline:
            update = f'{update} \n'

        # Add new output
        self.view.current_page.runtime_output.config(state='normal')
        self.view.current_page.runtime_output.insert(loc, update)
        self.view.current_page.runtime_output.config(state='disabled')

        self.view.current_page.tkraise()
