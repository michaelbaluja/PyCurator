Extending
=========

To utilize the collection functionality for a repository that is not already supported (see :doc:`collectors`),
there is a rich set of base classes that can be built upon for additional repositories. Currently, the base classes
provide interfaces for basic APIs, along with mixins for APIs with search term options and search type options.

.. currentmodule:: pycurator.collectors.base

.. autosummary::
    :toctree: _autosummary
    :recursive:

    BaseAPICollector
    BaseTermCollector
    BaseTermTypeCollector
    BaseTypeCollector
