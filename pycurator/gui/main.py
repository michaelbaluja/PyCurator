import tkinter as tk

from .MVC import CuratorView, CuratorController


class ScraperGUI(tk.Tk):
    def __init__(self):
        super().__init__()

        self.title('PyCurator')

        view = CuratorView(self)
        controller = CuratorController(view=view)

        self.bind('<Return>', controller.request_next_page)

        view.set_controller(controller)
        view.show()
