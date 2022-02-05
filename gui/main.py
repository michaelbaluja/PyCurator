import tkinter as tk
from .landing_page import LandingPage
from .selection_page import SelectionPage
from .run_page import RunPage


class ScraperGUI(tk.Frame):
    def __init__(self, *args, **kwargs):
        tk.Frame.__init__(self, *args, **kwargs)

        self.repo_params = dict()

        self.landing_page = LandingPage(self)
        self.selection_page = SelectionPage(self)
        self.run_page = RunPage(self)

        container = tk.Frame(self)
        container.pack(side='top', fill='both', expand=True)

        self.landing_page.place(
            in_=container,
            x=0,
            y=0,
            relwidth=1,
            relheight=1
        )
        self.selection_page.place(
            in_=container,
            x=0,
            y=0,
            relwidth=1,
            relheight=1
        )
        self.run_page.place(
            in_=container,
            x=0,
            y=0,
            relwidth=1,
            relheight=1
        )

        self.landing_page.show()
