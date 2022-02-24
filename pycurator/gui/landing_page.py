import os

import tkinter as tk
import tkinter.ttk as ttk

from .bases import ViewPage
from .selection_page import SelectionPage


class LandingPage(ViewPage):
    def __init__(self, *args, **kwargs):
        ViewPage.__init__(self, *args, **kwargs)

    @ViewPage.no_overwrite
    def show(self):
        # Landing page information
        label = ttk.Label(
            self,
            text='PyCurator',
            font='helvetica 16 bold'
        )

        # Load landing page text
        msg_path = os.path.join('pycurator', 'gui', 'landing_msg.txt')
        with open(msg_path) as f:
            message = tk.StringVar(value=f.read())

        message_box = tk.Message(self, textvariable=message)

        # Create continue button & bind to Enter key
        self.next_page_button = ttk.Button(
            self,
            text='Continue',
            command=lambda: self.controller.show(SelectionPage)
        )

        # Arrange elements
        label.grid(row=0)
        message_box.grid(row=1)
        self.next_page_button.grid(row=2)

        self.grid(row=0, column=0, sticky='nsew')
        self.tkraise()
