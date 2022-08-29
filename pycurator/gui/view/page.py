"""
Module for UI page base class.
"""

from abc import abstractmethod
from tkinter import ttk
from typing import Any, Callable, NoReturn


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
        self.next_page = kwargs.pop("next_page", None)
        ttk.Frame.__init__(self, *args, **kwargs)

        self.controller = None
        self.is_initialized = False
        self.next_page_button = None

    def set_controller(self, controller: Any) -> None:
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
        """Abstract placeholder method for resetting page frame."""
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
