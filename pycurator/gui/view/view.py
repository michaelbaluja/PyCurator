"""Module for creating View of MVC structure."""

from __future__ import annotations

import itertools as it
import tkinter as tk
from tkinter import ttk
from typing import Type, TYPE_CHECKING

from . import landing_page, run_page, selection_page, page

if TYPE_CHECKING:
    from .. import controller


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
        self.current_page = None

        self.grid_rowconfigure(0, weight=1)
        self.grid_columnconfigure(0, weight=1)
        self.grid(row=0, column=0, sticky="nsew")

        page_order = (
            landing_page.LandingPage,
            selection_page.SelectionPage,
            run_page.RunPage,
        )
        self.pages = {
            C: C(self, next_page=N) for C, N in it.pairwise(page_order + (None,))
        }

    def set_controller(self, curator_controller: controller.CuratorController) -> None:
        """Setter for UI Controller of View object."""
        for page_ in self.pages.values():
            page_.set_controller(curator_controller)

    def show(self, page_: Type[page.ViewPage] = landing_page.LandingPage) -> None:
        """Display the provided page on the UI."""
        self.current_page = self.pages[page_]
        self.current_page.show()
