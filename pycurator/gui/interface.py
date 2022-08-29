"""
Module for creating base classes of UI.
"""

from __future__ import annotations

import tkinter as tk

from . import controller, view


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

    def __init__(self) -> None:
        super().__init__()

        self.title("PyCurator")

        curator_view = view.view.CuratorView(self)
        curator_controller = controller.CuratorController(view_=curator_view)

        self.bind("<Return>", curator_controller.request_next_page)

        curator_view.set_controller(curator_controller)
        curator_view.show()
