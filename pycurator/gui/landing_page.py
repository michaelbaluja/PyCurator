#
#  This software is Copyright © 2021-2022 The Regents of the University of California.

import tkinter as tk
import tkinter.ttk as ttk
from textwrap import dedent
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

        message = tk.StringVar(
            value=dedent(
                """  
                Making repository research curation as easy as py.
    
                This software is Copyright © 2021-2022 
                The Regents of the University of California.
                All Rights Reserved.
                For full licensing information, reference the LICENSE present
                in the PyCurator repository.
                
                The initial development of this program was funded by the
                Librarians Association of the University of California (LAUC)
                and UC San Diego Library Research Data Curation Program (RDCP).
                """
            )
        )

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
