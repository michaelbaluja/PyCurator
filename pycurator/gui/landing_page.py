import os
import tkinter as tk
import tkinter.ttk as ttk
from typing import Any, NoReturn

from .base import ViewPage
from .selection_page import SelectionPage


class LandingPage(ViewPage):
    """Landing Page of the PyCurator View component.

    Parameters
    ----------
    *args : tuple, optional
        Positional arguments for ViewPage constructor, which further
        passes to ttk.Frame.
    **kwargs : dict, optional
        Keyword arguments for ViewPage constructor, which further
        passes to ttk.Frame.

    See Also
    --------
    pycurator.gui.base.ViewPage
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

    @ViewPage.no_overwrite
    def show(self) -> None:
        """Arrange and display Landing Page of PyCurator UI."""
        label = ttk.Label(
            self,
            text='PyCurator',
            font='helvetica 16 bold'
        )

        msg_path = os.path.join('pycurator', 'gui', 'landing_msg.txt')
        with open(msg_path) as f:
            message = tk.StringVar(value=f.read())

        message_box = tk.Message(self, textvariable=message)

        self.next_page_button = ttk.Button(
            self,
            text='Continue',
            command=lambda: self.controller.show(SelectionPage)
        )

        label.grid(row=0, pady=7)
        message_box.grid(row=1, padx=10)
        self.next_page_button.grid(row=2, pady=5)
        self.grid(row=0, column=0, sticky='nsew')
        self.tkraise()

    def reset_frame(self) -> NoReturn:
        pass
