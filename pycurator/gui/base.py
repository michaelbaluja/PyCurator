from __future__ import annotations

import threading
import tkinter as tk
import tkinter.ttk as ttk
from abc import abstractmethod
from collections.abc import Callable
from typing import Any, NoReturn

import pycurator.collectors
import pycurator.gui


class PyCuratorUI(tk.Tk):
    """UI for PyCurator Application.

    The UI is structured in a Model-View-Controller format. The View
    is separated into a Landing Page with license information, a
    Selection Page with repository options and related runtime
    parameters, and a Run Page providing real-time updates on the status
    of the data collection task.

    See Also
    --------
    pycurator.gui.MVC :
        Module containing the Model, View, and Controller classes.
    """

    def __init__(self):
        super().__init__()

        self.title('PyCurator')

        view = pycurator.gui.CuratorView(self)
        controller = pycurator.gui.CuratorController(view=view)

        self.bind('<Return>', controller.request_next_page)

        view.set_controller(controller)
        view.show()


class ThreadedRun(threading.Thread):
    """Wrapper for concrete Collector object to allow threading.

    Parameters
    ----------
    collector : Subclass of BaseCollector
    **kwargs : dict, optional
        Additional parameters to pass to the run function of the given
        collector object. Current integrated examples include the
        save_type of the output file and save_dir for storing the
        output file.

    See Also
    --------
    run :
        Start-to-finish pipeline for running Collectors. See specific
        repository Collector classes for more concrete details.
    """

    def __init__(
            self,
            collector: pycurator.collectors.BaseCollector,
            **kwargs: Any
    ) -> None:
        self.collector = collector
        super().__init__(target=self.collector.run, **kwargs)


class ViewPage(ttk.Frame):
    """Base for page in UI View object.

    Parameters
    ----------
    *args : tuple, optional
        Positional arguments to pass to ttk.Frame constructor.
    **kwargs : dict, optional
        Keyword arguments to pass to ttk.Frame constructor.

    Attributes
    ----------
    controller : MVC.CuratorController
        Default of None to allow UI instantiation of pages before
        Controller.
    is_initialized : bool
    next_page_button : None or ttk.Button
        Widget with action linking to the next sequential page of the UI.

    Methods
    -------
    set_controller
    no_overwrite
        Decorator for loading a page without overwriting the previously
        displayed values. Useful for reloading a page without destroying
        widgets or reconstructing an instance of the page.
    reset_frame
    attempt_raise
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        ttk.Frame.__init__(self, *args, **kwargs)

        self.controller = None
        self.is_initialized = False
        self.next_page_button = None

    def set_controller(
            self,
            controller: pycurator.gui.MVC.CuratorController
    ) -> None:
        """Setter for Controller element of PyCurator UI."""
        self.controller = controller

    @staticmethod
    def no_overwrite(show_func: Callable[[Any], Any]) -> Callable[[Any], Any]:
        """Wrapper for page display without element overwrite."""
        def display(self) -> None:
            raised = self.attempt_raise()
            if not raised:
                show_func(self)
                self.is_initialized = True

        return display

    @abstractmethod
    def reset_frame(self) -> NoReturn:
        raise NotImplementedError

    def attempt_raise(self) -> bool:
        """Display page if initialized."""
        if self.is_initialized:
            try:
                self.reset_frame()
            except AttributeError:
                pass

            self.tkraise()
            return True
        return False
