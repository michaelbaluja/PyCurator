#   PyCurator LGPL 3.0 <https://www.gnu.org/licenses/lgpl-3.0.txt>
#   Copyright (c) 2022. Michael Baluja

import logging
from collections.abc import Callable, Collection
from typing import Any, Union, TypeVar

from typing_extensions import ParamSpec

T = TypeVar('T')
P = ParamSpec('P')


def extract_parameter(
        base,
        func: Callable[P, T],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        param: str
):
    if param in kwargs:
        kwargs[param] = base._validate(kwargs.get(param))
    else:
        try:
            func_params = func.__code__.co_varnames
            idx = func_params.index(param)
            if 'self' in func_params:
                idx -= 1
            args = list(args)
            args[idx] = base._validate(args[idx])
            args = tuple(args)
        except ValueError:
            logging.debug(
                f'Attempting to validate \'{param}\' where one does not'
                ' appear to be present.'
            )
    return args, kwargs


def validate_metadata_parameters(
        object_paths: Union[str, Collection[str]]
) -> Collection[str]:
    """Ensures that the metadata object paths are of the proper form.

    Parameters
    ----------
    object_paths : str or Collection of str

    Returns
    -------
    object_paths : str or Collection of str

    Raises
    ------
    TypeError
        If object paths are not all str instance.
    """

    if isinstance(object_paths, str):
        object_paths = [object_paths]
    if not all([isinstance(path, str) for path in object_paths]):
        raise TypeError('All object paths must be of type str.')

    return object_paths


def validate_save_filename(filename: str) -> str:
    """Remove quotations from filename, replace spaces with underscore."""
    return filename.replace('"', '').replace("'", '').replace(' ', '_')
