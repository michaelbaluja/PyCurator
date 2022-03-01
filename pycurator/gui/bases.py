from __future__ import annotations
from abc import abstractmethod
import threading
import tkinter.ttk as ttk
from typing import Any, NoReturn
from collections.abc import Callable

import pycurator.scrapers
import pycurator.gui


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
