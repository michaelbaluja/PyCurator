"""
Module for creating UI Landing Page.
"""

import tkinter as tk
from textwrap import dedent
from tkinter import ttk
from typing import NoReturn

from . import page


class LandingPage(page.ViewPage):
    """Landing Page of the PyCurator View component.

    See Also
    --------
    pycurator.gui.base.ViewPage
    """

    @page.ViewPage.no_overwrite
    def show(self) -> None:
        """Arrange and display Landing Page of PyCurator UI."""
        label = ttk.Label(self, text="PyCurator", font="helvetica 16 bold")

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
            self, text="Continue", command=lambda: self.controller.show(self.next_page)
        )

        label.grid(row=0, pady=7)
        message_box.grid(row=1, padx=10)
        self.next_page_button.grid(row=2, pady=5)
        self.grid(row=0, column=0, sticky="nsew")
        self.tkraise()

    def reset_frame(self) -> NoReturn:
        pass
