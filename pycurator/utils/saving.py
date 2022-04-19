import os
from typing import Union

import pandas as pd

from .validating import validate_save_filename

save_options = {
    'CSV': '.csv',
    'JSON': '.json',
    'Pickle': '.pkl'
}
try:
    import openpyxl

    save_options['Excel'] = '.xlsx'
    save_options = dict(sorted(save_options.items()))
except ImportError:
    pass
try:
    import pyarrow

    save_options['Parquet'] = '.parquet'
    save_options['Feather'] = '.feather'
    save_options = dict(sorted(save_options.items()))
except ImportError:
    pass


def save_results(
        results: dict,
        data_dir: Union[str, os.PathLike[str]],
        output_format: str
) -> None:
    """Export DataFrame objects to specified directory.

    Parameters
    ----------
    results : dict
        Dictionary of pandas.DataFrame objects to be saved.
    data_dir : str or PathLike str
        Path for parent directory to save files to.
    output_format : str
        Format for saved results. Acceptable options are seen in
        save_options.

    Raises
    ------
    TypeError
        "results" not of type dict or "datadir" not of type str.
    ValueError
        Incorrect output_format provided.

    See Also
    --------
    os.PathLike : ABC for objects representing a file system path.
    save_options : Output format to extension dict.
    """

    if not isinstance(results, dict):
        raise TypeError(
            f'results must be of type dict, not \'{type(results)}\'.'
        )
    if not isinstance(data_dir, str):
        raise TypeError(
            f'data_dir must of type str, not \'{type(data_dir)}\'.'
        )

    try:
        extension = save_options[output_format]
    except KeyError:
        raise ValueError(
            f'output_format must be one of {list(save_options.keys())}, '
            f'not \'{output_format}\'.'
        )

    if not os.path.isdir(data_dir):
        os.makedirs(data_dir)

    for query, df in results.items():
        if isinstance(query, str):
            output_filename = f'{query}{extension}'
        else:
            search_term, search_type = query
            output_filename = f'{search_term}_{search_type}{extension}'

        output_filename = validate_save_filename(
            output_filename
        )

        save_dataframe(
            results=df,
            filepath=os.path.join(data_dir, output_filename),
            output_format=output_format
        )


def save_dataframe(
        results: pd.DataFrame,
        filepath: Union[str, os.PathLike[str]],
        output_format: str
) -> None:
    """Saves the specified results to the file provided.

    Parameters
    ----------
    results : pandas.DataFrame
        If DataFrame, results will be stored in a csv format.
    filepath : str or PathLike str
        Path to save file. Take note of output type as specified
        above, as appending the incorrect filetype may result in the
        file being unreadable.
    output_format : str
        Format for saved results. Acceptable options are seen in
        save_options.

    Raises
    ------
    ValueError
        If a non-dataframe object results or an incorrect output_format
        is provided.

    See Also


    Examples
    --------
    >>> data = {"id": [123, 432, 576, 194], "value": [0, 1, 1, 0]}
    >>> save_dataframe(
    ...     results=data,
    ...     filepath='data/id_vals.json',
    ...     output_format='JSON'
    ... )
    Traceback (most recent call last):
        ...
    ValueError: 'results' must be of type pandas.DataFrame, not 'dict'.

    >>> df1 = pd.DataFrame(data)
    >>> save_dataframe(
    ...     results=df1,
    ...     filepath='data/id_vals.json',
    ...     output_format='JSON'
    ... )

    >>> df2 = df1
    >>> df2.loc[:, 'Name'] = ('Jeff', 'Scott', 'Will', 'Dan')
    >>> save_dataframe(
    ...     results=df2,
    ...     filepath='data/updated_id_vals.csv',
    ...     output_format='CSV'
    ... )

    >>> save_dataframe(
    ...     results=df2,
    ...     filepath='data/full_data.txt',
    ...     output_format='TXT'
    ... )
    Traceback (most recent call last):
        ...
    ValueError: 'TXT' is not supported. 'output_format' must be one of ['CSV', 'JSON', 'Pickle'].
    """

    if not isinstance(results, pd.DataFrame):
        raise ValueError(
            f'\'results\' must be of type pandas.DataFrame, not'
            f' \'{type(results)}\'.'
        )

    try:
        getattr(results, f'to_{output_format.lower()}')(filepath)
    except AttributeError:
        raise ValueError(
            f'\'{output_format}\' is not supported. \'output_format\''
            f' must be one of {list(save_options.values())}.'
        )
