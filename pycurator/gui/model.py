"""
Module for creating model for MVC format.
"""

import threading
from typing import Any, Type, ParamSpec

from ..collectors import base as collector_base

P = ParamSpec("P")


class ThreadedRun(threading.Thread):
    """Wrapper for concrete Collector object to allow threading.

    Parameters
    ----------
    collector : Subclass of BaseCollector
    **kwargs : dict, optional
        Additional parameters to pass to the run function of the given
        collector object. Current integrated examples include the
        save_type of the output file and save_dir for storing the
        output file.

    See Also
    --------
    run :
        Start-to-finish pipeline for running Collectors. See specific
        repository Collector classes for more concrete details.
    """

    def __init__(self, collector: collector_base.BaseCollector, **kwargs: Any) -> None:
        self.collector = collector
        super().__init__(target=self.collector.run, **kwargs)


class CollectorModel:
    """Model for the PyCurator UI. Extends the Collector class.

    Parameters
    ----------
    collector_class : Inherited BaseCollector class
    collector_name : str

    Attributes
    ----------
    collector_class : Inherited BaseCollector class
    collector_name : str
    collector : Inherited BaseCollector instance
    run_thread : threading.Thread
        Separate thread for data collection so that UI functionality
        is not interrupted.
    """

    def __init__(
        self, collector_class: Type[collector_base.BaseCollector], collector_name: str
    ) -> None:
        self.collector_class = collector_class
        self.collector_name = collector_name
        self.collector = None
        self.run_thread = None

        self.requirements = {
            "search_terms": issubclass(
                self.collector_class, collector_base.TermQueryMixin
            ),
            "search_types": issubclass(
                self.collector_class, collector_base.TypeQueryMixin
            ),
        }

    def initialize_collector(self, **param_val_kwargs: P.kwargs) -> None:
        """Instantiate Collector object from class and provided kwargs.

        Parameters
        ----------
        **param_val_kwargs : dict, optional
            Parameters for Collector initialization. See individual
            Collector classes for specifics.

        See Also
        --------
        pycurator.collectors : Classes for repository date collection.
        """

        self.collector = self.collector_class(**param_val_kwargs)

    def initialize_thread(self, thread_kwargs: P.kwargs) -> None:
        """Instantiate Collector Thread with runtime arguments.

        Parameters
        ----------
        thread_kwargs : dict, optional
            Parameters to provide to run function of collector.

        See Also
        -------
        pycurator.gui.base.ThreadedRun :
            Collector wrapper allowing for seamless UI performance by
            shifting collector actions to separate thread.
        """

        self.run_thread = ThreadedRun(collector=self.collector, kwargs=thread_kwargs)
