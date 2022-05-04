from __future__ import annotations

import _tkinter
import os
import queue
import tkinter
import tkinter as tk
import tkinter.ttk as ttk
from typing import Optional, ParamSpec, Type, TypeVar, Union

from pycurator import collectors
from pycurator._typing import AttributeKey, AttributeValue, TKVarValue
from .base import ThreadedRun, ViewPage
from .landing_page import LandingPage
from .run_page import RunPage
from .selection_page import SelectionPage

Page = TypeVar('Page', bound=ViewPage)
P = ParamSpec('P')


class CuratorView(ttk.Frame):
    """View for the PyCurator UI.

    Parameters
    ----------
    parent : tk.Tk

    Attributes
    ----------
    pages : dict[str, ViewPage]
        Map from page name to page object.
    current_page : ViewPage

    See Also
    --------
    tk.Frame
    tk.Tk
    """

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
        """Setter for UI Controller of View object."""
        for page in self.pages.values():
            page.set_controller(controller)

    def show(self, page: Type[Page] = LandingPage) -> None:
        """Display the provided page on the UI."""
        self.current_page = self.pages[page]
        self.current_page.show()


class CollectorModel:
    """Model for the PyCurator UI. Extends the Collector class.

    Parameters
    ----------
    collector_class : Inherited BaseCollector class
    collector_name : str

    Attributes
    ----------
    collector_class : Inherited BaseCollector class
    collector_name : str
    collector : Inherited BaseCollector instance
    run_thread : threading.Thread
        Separate thread for data collection so that UI functionality
        is not interrupted.
    """

    def __init__(
            self,
            collector_class: Type[collectors.BaseCollector],
            collector_name: str
    ) -> None:
        self.collector_class = collector_class
        self.collector_name = collector_name
        self.collector = None
        self.run_thread = None

        self.requirements = {
            'search_terms': issubclass(
                self.collector_class,
                collectors.TermQueryMixin
            ),
            'search_types': issubclass(
                self.collector_class,
                collectors.TypeQueryMixin
            )
        }

    def initialize_collector(self, **param_val_kwargs: P.kwargs) -> None:
        """Instantiate Collector object from class and provided kwargs.

        Parameters
        ----------
        **param_val_kwargs : dict, optional
            Parameters for Collector initialization. See individual
            Collector classes for specifics.

        See Also
        --------
        pycurator.collectors : Classes for repository date collection.
        """

        self.collector = self.collector_class(**param_val_kwargs)

    def initialize_thread(self, thread_kwargs: P.kwargs) -> None:
        """Instantiate Collector Thread with runtime arguments.

        Parameters
        ----------
        thread_kwargs : dict, optional
            Parameters to provide to run function of collector.

        See Also
        -------
        pycurator.gui.base.ThreadedRun :
            Collector wrapper allowing for seamless UI performance by
            shifting collector actions to separate thread.
        """

        self.run_thread = ThreadedRun(
            collector=self.collector,
            kwargs=thread_kwargs
        )


class CuratorController:
    """Controller for the PyCurator UI.

    Parameters
    ----------
    model : CollectorModel, optional (default=None)
        Model for the PyCurator UI.
    view : CuratorView, optional (default=None)
        View for the PyCurator UI.

    Attributes
    ----------
    model : CollectorModel
    view : CuratorView
    runtime_param_vars : dict of parameter name to parameter value

    See Also
    --------
    CollectorModel
    CuratorView
    """

    def __init__(
            self,
            model: Optional[CollectorModel] = None,
            view: Optional[CuratorView] = None
    ) -> None:
        self.model = model
        self.view = view

        # Create placeholder for runtime_requirements property attribute
        self._runtime_requirements = None

        self.runtime_param_vars = dict()

    def request_next_page(self, *args: P.args) -> None:
        """Trigger the UI button corresponding to advancing pages."""
        try:
            self.view.current_page.next_page_button.invoke()
        except AttributeError:
            pass

    def request_execution(self) -> None:
        """Propagate Collector execution from UI to Collector."""
        self.model.collector.request_execution()

    def show(self, page: Type[Page]) -> None:
        """Show the provided page.

        Helper function for UI elements to request a page be shown
        without accessing the view component directly.

        Parameters
        ----------
        page : ViewPage
        """

        self.view.show(page)

    def set_model(
            self,
            collector: Type[collectors.BaseCollector],
            collector_name: str
    ) -> None:
        """Dynamically set the model component and helper variables.

        Parameters
        ----------
        collector : Inherited BaseCollector Class
        collector_name : str
        """

        self.model = CollectorModel(collector, collector_name)
        self.runtime_requirements = self.model.requirements
        self.runtime_param_vars[collector_name] = dict()

    def add_run_parameter(
            self,
            param: AttributeKey,
            value: AttributeValue
    ) -> None:
        """Setter for runtime_param_vars dict.

        Parameters provided are dynamically linked to the Collector
        currently loaded by the model. This allows values to be cached
        for later use, regardless of which Collectors are ran.

        Parameters
        ----------
        param : str
            Key for storing parameter value.
        value
        """

        self.runtime_param_vars[self.model.collector_name][param] = value

    def get_run_parameter(self, param: AttributeKey) -> AttributeValue:
        """Getter for runtime_param_vars dict of the current Collector."""
        return self.runtime_param_vars[self.model.collector_name][param]

    @property
    def runtime_requirements(self) -> dict[str, bool]:
        """Getter for requirements of the model's current Collector."""
        return self._runtime_requirements

    @runtime_requirements.setter
    def runtime_requirements(self, requirements: dict[str, bool]) -> None:
        """Setter for requirements of the model's current Collector."""
        self._runtime_requirements = requirements

    def evaluate_parameter(
            self,
            param_name: str
    ) -> Union[list[TKVarValue], TKVarValue, str]:
        """Retrieves values from the runtime_param_vars.

        Parameters
        ----------
        param_name : str

        Returns
        -------
        TKVarValue or Any
            For tkinter variables assigned to a parameter, returns the
            value from the tkinter.Variable.get() method.
            For non-tkinter variables, returns the variable itself.
        """

        param_var = self.get_run_parameter(param_name)

        if isinstance(param_var, dict):
            return [key for key, val in param_var.items() if val.get()]
        elif isinstance(param_var, tkinter.Variable):
            return param_var.get()
        else:
            return param_var

    def parse_run_parameters(self) -> None:
        """Ensure necessary requirements are present before running.

        See Also
        --------
        pycurator.collectors.base
        """

        param_val_kwargs = {
            param_name: self.evaluate_parameter(param_name)
            for param_name in self.runtime_param_vars[
                self.model.collector_name
            ]
        }

        # Add default save directory
        if not param_val_kwargs.get('save_dir'):
            param_val_kwargs['save_dir'] = os.path.join(
                'data',
                self.model.collector_name
            )
            os.makedirs(param_val_kwargs['save_dir'], exist_ok=True)

        missing_reqs = [
            param_name
            for param_name, is_req in self.runtime_requirements.items()
            if is_req and not param_val_kwargs.get(param_name)
        ]

        if missing_reqs:
            self.view.pages[SelectionPage].alert_missing_reqs(missing_reqs)
        else:
            self.initialize_run(**param_val_kwargs)

    def initialize_run(self, **param_val_kwargs: P.kwargs) -> None:
        """Initialize the model Collector and Thread for collection run.

        Parameters
        ----------
        **param_val_kwargs : dict, optional
            Initialization and runtime parameters for the collector
            model. save_dir and save_type are removed from the dict and
            passed to the thread initialization, while the rest are
            passed to the collector initialization.

        See Also
        -------
        CollectorModel
        pycurator.gui.base.ThreadedRun
        """

        save_dir = param_val_kwargs.pop('save_dir')
        save_type = param_val_kwargs.pop('save_type')

        # Set up the run thread
        self.model.initialize_collector(**param_val_kwargs)
        self.model.initialize_thread(
            {
                'save_dir': save_dir,
                'save_type': save_type,
            }
        )

        self.view.show(RunPage)

    def run_collector(self) -> None:
        """Start collection and propagate updates to UI."""
        self.model.run_thread.start()
        self.view.after(100, self.process_runtime_updates)

    def process_runtime_updates(self) -> None:
        """Push status update from collector queue to output."""
        # Check for updates to progress bar
        if self.model.run_thread.collector.num_queries is not None:
            self._update_progress_bar()

        # Get next object in queue and push to output widget.
        try:
            msg = self.model.run_thread.collector.status_queue.get_nowait()

            self._update_runtime_output(msg)

            # Check if process still running
            if self.model.collector.continue_running:
                self.view.after(100, self.process_runtime_updates)
            else:
                # Empty queue
                while not self.model.run_thread.collector.status_queue.empty():
                    self.process_runtime_updates()

                # Remove query message
                self.view.current_page.progress_label['text'] = 'Complete'

                # Stop progress bar
                self._clear_progress_bar()

                # Reactivate back button
                self.view.current_page.back_button.config(state='normal')

                # Replace Stop button with Exit button
                self.view.current_page.stop_button.pack_forget()
                self.view.current_page.exit_button.pack(side='left')

                return
        # If the queue is empty, continually check
        except queue.Empty:
            self.view.after(100, self.process_runtime_updates)

    def _clear_progress_bar(self) -> None:
        self.view.current_page.progress_bar.stop()
        self.view.current_page.progress_bar['mode'] = 'determinate'
        self.view.current_page.progress_bar['value'] = 0

    def _update_progress_bar_indeterminate(self) -> None:
        self.view.current_page.progress_bar['mode'] = 'indeterminate'
        if self.model.run_thread.collector.num_queries:
            self.view.current_page.progress_bar.start()
        else:
            self._clear_progress_bar()

    def _update_progress_bar_determinate(self) -> None:
        self.view.current_page.progress_bar.stop()
        self.view.current_page.progress_determinate_num['text'] = \
            f'({self.model.run_thread.collector.queries_completed}/' \
            f'{self.model.run_thread.collector.num_queries})'
        self.view.current_page.progress_bar['mode'] = 'determinate'
        self.view.current_page.progress_bar['value'] = \
            (self.model.run_thread.collector.queries_completed /
             self.model.run_thread.collector.num_queries * 100) + 1

    def _update_progress_bar(self) -> None:
        """Update status of progress bar based on collector status."""
        self.view.current_page.progress_label['text'] = \
            self.model.run_thread.collector.current_query_ref

        if isinstance(self.model.run_thread.collector.num_queries, bool):
            self._update_progress_bar_indeterminate()
        else:
            self._update_progress_bar_determinate()

    def _update_runtime_output(
            self,
            update: str,
            loc: _tkinter.Tcl_Obj | str | float | tk.Misc = 'end',
            newline: bool = True
    ) -> None:
        """Push Collector status to PyCurator UI window.

        Parameters
        ----------
        update : str
            Status to output.
        loc _tkinter.Tcl_Obj or str or float or tk.Misc
                optional (default='end')
            Position to place update at.
        newline : bool, optional (default=True)
            Flag for adding a newline to output. Default to True in
            order for updates to appear one after the other for easy
            user digest.
        """

        if newline:
            update = f'{update} \n'

        # Add new output
        self.view.current_page.runtime_output.config(state='normal')
        self.view.current_page.runtime_output.insert(loc, update)
        self.view.current_page.runtime_output.config(state='disabled')

        self.view.current_page.tkraise()
