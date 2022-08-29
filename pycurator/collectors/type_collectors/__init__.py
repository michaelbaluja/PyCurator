"""
Load modules into parent package.
"""

try:
    from .open_ml import OpenMLCollector
except (ImportError, ModuleNotFoundError):
    pass
