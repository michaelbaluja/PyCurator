from __future__ import annotations

import threading
import tkinter as tk
import tkinter.ttk as ttk
from abc import abstractmethod
from collections.abc import Callable
from typing import Any, NoReturn

import pycurator.gui
import pycurator.scrapers


class ScraperGUI(tk.Tk):
    def __init__(self):
        super().__init__()

        self.title('PyCurator')

        view = pycurator.gui.CuratorView(self)
        controller = pycurator.gui.CuratorController(view=view)

        self.bind('<Return>', controller.request_next_page)

        view.set_controller(controller)
        view.show()


class ThreadedRun(threading.Thread):
    def __init__(
            self,
            scraper: pycurator.scrapers.AbstractScraper,
            **kwargs: Any
    ) -> None:
        self.scraper = scraper
        super().__init__(target=self.scraper.run, **kwargs)


class ViewPage(ttk.Frame):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        ttk.Frame.__init__(self, *args, **kwargs)

        self.controller = None
        self.is_initialized = False
        self.next_page_button = None

    def set_controller(
            self,
            controller: pycurator.gui.MVC.CuratorController
    ) -> None:
        self.controller = controller

    @staticmethod
    def no_overwrite(show_func: Callable[[Any], Any]) -> Callable[[Any], Any]:
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
        if self.is_initialized:
            try:
                self.reset_frame()
            except AttributeError:
                pass

            self.tkraise()
            return True
        return False
