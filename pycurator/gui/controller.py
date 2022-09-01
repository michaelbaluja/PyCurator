"""
Module for UI Model View Controller structure.
"""

from __future__ import annotations

import os
import queue
import tkinter as tk
from typing import Optional, ParamSpec, Type, Union
import _tkinter

from . import view, model
from pycurator._typing import AttributeKey, AttributeValue, TKVarValue
from pycurator.collectors import base as collector_base

P = ParamSpec("P")


class CuratorController:
    """Controller for the PyCurator UI.

    Parameters
    ----------
    model_ : CollectorModel, optional
        Model for the PyCurator UI.
    view_ : CuratorView, optional
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
            model_: Optional[model.CollectorModel] = None,
            view_: Optional[view.view.CuratorView] = None,
    ) -> None:
        self.model = model_
        self.view = view_

        # Create placeholder for runtime_requirements property attribute
        self._runtime_requirements = None

        self.runtime_param_vars = {}

    def request_next_page(self, *args: P.args) -> None:
        """Trigger the UI button corresponding to advancing pages."""
        try:
            self.view.current_page.next_page_button.invoke()
        except AttributeError:
            pass

    def request_execution(self) -> None:
        """Propagate Collector execution from UI to Collector."""
        self.model.collector.request_execution()

    def show(self, page_: Type[view.page.ViewPage]) -> None:
        """Show the provided page.

        Helper function for UI elements to request a page be shown
        without accessing the view component directly.

        Parameters
        ----------
        page_ : ViewPage
        """

        self.view.show(page_)

    def set_model(
            self, collector: Type[collector_base.BaseCollector], collector_name: str
    ) -> None:
        """Dynamically set the model component and helper variables.

        Parameters
        ----------
        collector : Inherited BaseCollector Class
        collector_name : str
        """

        self.model = model.CollectorModel(collector, collector_name)
        self.runtime_requirements = self.model.requirements
        self.runtime_param_vars[collector_name] = {}

    def add_run_parameter(self, param: AttributeKey, value: AttributeValue) -> None:
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
            self, param_name: str
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
            param_name = [key for key, val in param_var.items() if val.get()]
        elif isinstance(param_var, tk.Variable):
            param_name = param_var.get()
        else:
            param_name = param_var

        return param_name

    def parse_run_parameters(self) -> None:
        """Ensure necessary requirements are present before running.

        See Also
        --------
        pycurator.collectors.base
        """

        param_val_kwargs = {
            param_name: self.evaluate_parameter(param_name)
            for param_name in self.runtime_param_vars[self.model.collector_name]
        }

        # Add default save directory
        if not param_val_kwargs.get("save_dir"):
            param_val_kwargs["save_dir"] = os.path.join(
                "data", self.model.collector_name
            )
            os.makedirs(param_val_kwargs["save_dir"], exist_ok=True)

        missing_reqs = [
            param_name
            for param_name, is_req in self.runtime_requirements.items()
            if is_req and not param_val_kwargs.get(param_name)
        ]

        if missing_reqs:
            self.view.pages[view.selection_page.SelectionPage].alert_missing_reqs(
                missing_reqs
            )
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

        save_dir = param_val_kwargs.pop("save_dir")
        save_type = param_val_kwargs.pop("save_type")

        # Set up the run thread
        self.model.initialize_collector(**param_val_kwargs)
        self.model.initialize_thread(
            {
                "save_dir": save_dir,
                "save_type": save_type,
            }
        )

        self.view.show(view.run_page.RunPage)

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
                self.view.current_page.progress_label["text"] = "Complete"

                # Stop progress bar
                self._clear_progress_bar()

                # Reactivate back button
                self.view.current_page.back_button.config(state="normal")

                # Replace Stop button with Exit button
                self.view.current_page.stop_button.pack_forget()
                self.view.current_page.exit_button.pack(side="left")

                return
        # If the queue is empty, continually check
        except queue.Empty:
            self.view.after(100, self.process_runtime_updates)

    def _clear_progress_bar(self) -> None:
        self.view.current_page.progress_bar.stop()
        self.view.current_page.progress_bar["mode"] = "determinate"
        self.view.current_page.progress_bar["value"] = 0

    def _update_progress_bar_indeterminate(self) -> None:
        self.view.current_page.progress_bar["mode"] = "indeterminate"
        if self.model.run_thread.collector.num_queries:
            self.view.current_page.progress_bar.start()
        else:
            self._clear_progress_bar()

    def _update_progress_bar_determinate(self) -> None:
        self.view.current_page.progress_bar.stop()
        self.view.current_page.progress_determinate_num["text"] = (
            f"({self.model.run_thread.collector.queries_completed}/"
            f"{self.model.run_thread.collector.num_queries})"
        )
        self.view.current_page.progress_bar["mode"] = "determinate"
        self.view.current_page.progress_bar["value"] = (
                self.model.run_thread.collector.queries_completed
                / self.model.run_thread.collector.num_queries
                * 100
                + 1
        )

    def _update_progress_bar(self) -> None:
        """Update status of progress bar based on collector status."""
        self.view.current_page.progress_label[
            "text"
        ] = self.model.run_thread.collector.current_query_ref

        if isinstance(self.model.run_thread.collector.num_queries, bool):
            self._update_progress_bar_indeterminate()
        else:
            self._update_progress_bar_determinate()

    def _update_runtime_output(
            self,
            update: str,
            loc: _tkinter.Tcl_Obj | str | float | tk.Misc = "end",
            newline: bool = True,
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
            update = f"{update} \n"

        # Add new output
        self.view.current_page.runtime_output.config(state="normal")
        self.view.current_page.runtime_output.insert(loc, update)
        self.view.current_page.runtime_output.config(state="disabled")

        self.view.current_page.tkraise()
