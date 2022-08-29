"""
Module for validating general function inputs.
"""

from collections.abc import Callable, Collection, Iterable
from typing import Any, Optional, ParamSpec, Type, TypeVar, Union

T = TypeVar("T")
P = ParamSpec("P")
Key = TypeVar("Key")
Value = TypeVar("Value")


def sort_dict_by_keys(unsorted_dict: dict[Key, Value]) -> dict[Key, Value]:
    """Sorts a dictionary by key values."""

    return dict(map(tuple, sorted(unsorted_dict.items())))


def is_all_type(objects: Iterable[Any], types: Union[Type[Any], tuple[Union[Type[Any], None], ...]]) -> bool:
    """Validate that iterable only contains objects of a given type or types.

    Parameters
    ----------
    objects : Iterable of Any
        Iterable of objects to type-check.
    types : type or tuple of types

    Returns
    -------
    bool

    Examples
    --------
    >>> is_all_type([1,2,3,4], int)
    True
    >>> is_all_type(["hello", "world", 123], (str, int))
    False
    """

    return all(isinstance(obj, types) for obj in objects)


def validate_from_arguments(
        base,
        param: str,
        func: Callable[P, T],
        args: Optional[tuple[Any, ...]] = None,
        kwargs: Optional[dict[str, Any]] = None,
):
    """Validate arguments for wrapper functions."""

    if not (args or kwargs):
        raise ValueError("args or kwargs must be provided to extract.")

    # Checking for param in kwargs
    if kwargs:
        if not isinstance(kwargs, dict):
            raise TypeError(
                f"kwargs parameter must be of type 'dict', not '{type(kwargs)}'."
            )
        if param in kwargs:
            kwargs[param] = base._validate(kwargs.get(param))
    else:
        kwargs = {}

    # Checking for param in args
    if args:
        if not isinstance(args, tuple):
            raise TypeError(
                f"args parameter must be of type 'set', not '{type(args)}'."
            )
        try:
            func_params = func.__code__.co_varnames
            idx = func_params.index(param)
            if "self" in func_params:
                idx -= 1
            args = list(args)
            args[idx] = base._validate(args[idx])
            args = tuple(args)
        except (IndexError, ValueError):
            pass
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
    if not all(isinstance(path, str) for path in object_paths):
        raise TypeError("All object paths must be of type str.")

    return object_paths


def validate_save_filename(filename: str) -> str:
    """Remove quotations from filename, replace spaces with underscore."""
    return filename.replace('"', "").replace("'", "").replace(" ", "_")
