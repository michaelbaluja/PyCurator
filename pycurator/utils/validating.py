import logging
from collections.abc import Callable, Collection
from typing import Any, Optional, ParamSpec, TypeVar, Union


T = TypeVar('T')
P = ParamSpec('P')


def validate_from_arguments(
        base,
        param: str,
        func: Callable[P, T],
        args: Optional[tuple[Any, ...]] = None,
        kwargs: Optional[dict[str, Any]] = None,
):
    if not (args or kwargs):
        raise ValueError('args or kwargs must be provided to extract.')

    # Checking for param in kwargs
    if kwargs:
        if not isinstance(kwargs, dict):
            raise TypeError(
                'kwargs parameter must be of type \'dict\', not'
                f' \'{type(kwargs)}\'.'
            )
        if param in kwargs:
            kwargs[param] = base._validate(kwargs.get(param))
    else:
        kwargs = dict()

    # Checking for param in args
    if args:
        if not isinstance(args, tuple):
            raise TypeError(
                'args parameter must be of type \'set\', not' 
                ' \'{type(args)}\'.'
            )
        try:
            func_params = func.__code__.co_varnames
            idx = func_params.index(param)
            if 'self' in func_params:
                idx -= 1
            args = list(args)
            args[idx] = base._validate(args[idx])
            args = tuple(args)
        except (IndexError, ValueError):
            logging.debug(
                f'Attempting to validate \'{param}\' where one does not'
                ' appear to be present.'
            )
    else:
        args = set()

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
