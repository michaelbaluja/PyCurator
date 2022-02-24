import pandas as pd
import re
import tkinter.ttk as ttk

month_name_to_integer = {
    'Jan': '01',
    'Feb': '02',
    'Mar': '03',
    'Apr': '04',
    'May': '05',
    'Jun': '06',
    'Jul': '07',
    'Aug': '08',
    'Sep': '09',
    'Oct': '10',
    'Nov': '11',
    'Dec': '12'
}


def button_label_frame(root, label_text, button_text, button_command):
    _frame = ttk.Frame(root)
    _label = ttk.Label(_frame, text=label_text)
    _button = ttk.Button(_frame, text=button_text, command=button_command)

    _label.grid(row=0, column=0)
    _button.grid(row=0, column=1)
    _frame.grid(sticky='w', columnspan=2)


def parse_numeric_string(entry, cast=False):
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


def find_first_match(string, pattern):
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


def is_nested(col):
    """Given an iterable, returns True if any item is a dictionary or list."""
    return any([True for x in col if type(x) in (dict, list)])


def select_from_files(root, selection_type, **kwargs):
    """Allows user to select local file/directory.

    Parameters
    ----------
    root : Tkinter.Frame
        Derived Frame class that contains UI.

        If choosing file:
            Root must contain "files" dict attribute to hold the selected file.
    selection_type : str
        Type of selection to be made.
        Examples include "credentials", "directory", "css_paths" etc.
    kwargs : dict, optional
        Allows users to input filetype restrictions.

    Raises
    ------
    NotImplementedError
        Occurs when called on a root that does not contain a files
        attribute.

    Notes
    -----
    If repo_name is passed, files are stored in the format
        root.files[repo_name][filetype] = filename
    If repo_name is None, files are stored in the format
        root.files[repo_name] = filename
    """

    from tkinter import filedialog as fd

    if kwargs.get('filetypes'):
        filetypes = kwargs.get('filetypes')
    else:
        filetypes = [
            ('All File Types', '*.*')
        ]

    if 'dir' in selection_type:
        selection = fd.askdirectory(
            title='Choose Directory',
            initialdir='.'
        )
    else:
        selection = fd.askopenfilename(
            title='Choose File',
            initialdir='.',
            filetypes=filetypes
        )

    try:
        root.controller.add_run_parameter(selection_type, selection)
    except AttributeError:
        raise NotImplementedError(
            f'{root} must contain add_run_parameter() attribute.'
        )


def expand_series(series):
    """Given a Series of nested data, returns unnested data as DataFrame.

    Parameters
    ----------
    series : pandas.Series
        Data to be unnested.

    Returns
    -------
    col_expand : pandas.DataFrame
        Column expanded to DataFrame.
    """

    # Ensure variable is of proper type
    try:
        assert isinstance(series, pd.Series)
    except AssertionError:
        raise ValueError(
            f'series must be of type pandas.Series, not \'{type(series)}\'.'
        )

    # Expand column
    col_expand = series.apply(pd.Series).dropna(axis=1, how='all')
    col_name = series.name

    # Rename expanded columns to clarify source
    # add source column name prefix to each new column to identify source
    col_expand_names = [
        f'{col_name}_{nested_col}' for nested_col in col_expand.columns
    ]
    # rename columns
    col_expand.columns = col_expand_names

    return col_expand


def flatten_nested_df(df):
    """Takes in a DataFrame and flattens any nested columns.

    Parameters
    ----------
    df : pandas.DataFrame

    Returns
    -------
    pandas.DataFrame
    """
    # If the df is empty, then we want to return
    if df.empty:
        return df
    # If the df is a Series, convert to DataFrame
    if isinstance(df, pd.Series):
        df = pd.DataFrame(df)
    # If there's only one column & it's not nested, return it
    if df.shape[1] == 1:
        if not is_nested(df.iloc[:, 0]):
            return df

    # Slice the first column off of the DataFrame
    first_col, df = df.iloc[:, 0], df.iloc[:, 1:]

    # If the first column is nested, unnest it
    if is_nested(first_col):
        first_col = expand_series(first_col)

    # Want to flatten the first column again (in case nested) and
    # combine with the rest of the flattened df
    return flatten_nested_df(first_col).join(flatten_nested_df(df))
