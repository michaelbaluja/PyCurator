#   PyCurator LGPL 3.0 <https://www.gnu.org/licenses/lgpl-3.0.txt>
#   Copyright (c) 2022. Michael Baluja

import re
from collections.abc import Collection
from typing import AnyStr, Union


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


def _validate_save_filename(filename: str) -> str:
    """Remove quotations from filename, replace spaces with underscore."""
    return filename.replace('"', '').replace("'", '').replace(' ', '_')


def parse_numeric_string(
        entry: AnyStr,
        cast: bool = False
) -> Union[list[Union[str, int]], str, int, None]:
    """Given a string, returns all integer numeric substrings.

    Parameters
    ----------
    entry : str
    cast : bool, optional (default=False)
        Option for instances to be int type-casted

    Returns
    -------
    numeric_instances
        If none present, NoneType
        If one present, returns int
        If multiple present, returns list of ints
    """

    assert isinstance(entry, str)
    assert isinstance(cast, bool)

    numeric_instances = re.findall(r'\d+', entry)

    if cast:
        numeric_instances = [int(num_str) for num_str in numeric_instances]

    # Format return variable based on docstring description
    if len(numeric_instances) == 0:
        numeric_instances = None
    elif len(numeric_instances) == 1:
        numeric_instances = numeric_instances[0]

    return numeric_instances


def find_first_match(
        string: AnyStr,
        pattern: re.Pattern[AnyStr]
) -> Union[AnyStr, None]:
    """Finds the first occurrence of a regex pattern in a given string.

    Parameters
    ----------
    string : str
    pattern : str

    Returns
    -------
    str

    See Also
    --------
    re.search(), re.Match.group()
    """

    try:
        return re.search(pattern, string).group()
    except AttributeError:
        # Occurs if re.search returns None, as None has no attribute .group()
        return None
