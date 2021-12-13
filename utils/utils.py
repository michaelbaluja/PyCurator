import pandas as pd
from collections import OrderedDict

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

def is_nested(col):
    """Given an iterable, returns True if any item is a dictionary or list."""
    return any([True for x in col if type(x) in (dict, list, OrderedDict)])

def button_label_frame(root, label_text, button_text, button_command, frame_row, frame_column):
    from tkinter import Button, Frame, Label

    # Create frame to hold label and button
    _frame = Frame(root)

    _label = Label(_frame, text=label_text)
    _label.grid(row=0, column=0, pady=(0, 10), sticky='e')

    _button = Button(_frame, text=button_text, command=button_command)
    _button.grid(row=0, column=1, pady=(0, 10), sticky='w')

    # Align credential frame widgets together 
    _frame.grid(row=frame_row, column=frame_column, sticky='w')

def select_file(root, file_type, repo_name=None, **kwargs):
    """Allows user to select local file.

    Parameters
    ----------
    root : Tkinter.Frame
        Derived Frame class that contains UI. Must contain "files" dict
        attribute to hold the selected file.
    file_type : str
        Type of file to be selected. Examples include "credentials", 
        "path_file", etc.
    repo_name : str, optional (default=None)
        The name of the repository that the file is associated with. If None,
        the file is associated with all repositories. 
    kwargs : dict, optional
        Allows users to input file
    
    If repo_name is passed, files are stored in the format 
        root.files[repo_name][filetype] = filename
    If repo_name is None, files are stored in the format 
        root.files[repo_name] = filename
    """

    from tkinter import filedialog as fd
    from tkinter import Label

    if kwargs.get('filetypes'):
        filetypes = kwargs.get('filetypes')
    else:
        filetypes = [
            ('All File Types', '*.*')
        ]

    if file_type == 'directory':
        root.base_save_dir = fd.askdirectory(
            title='Choose Directory',
            initialdir='.'
        )
    else:
        filename = fd.askopenfilename(
            title='Choose File',
            initialdir='.',
            filetypes=filetypes
        )

        try:
            if repo_name:
                root.files[repo_name][file_type] = filename
            else:
                root.files[file_type] = filename
        except AttributeError:
            raise NotImplementedError(f'{root} must contain "files" attribute.')


def expand_series(series):
    """Given a pandas Series of nested data, returns unnested data as DataFrame.
    
    Parameters
    ----------
    series : pd.Series
        Series of nested data.
    
    Returns
    -------
    col_expand : pd.DataFrame
        DataFrame of expanded data.
    """

    # Ensure variable is of proper type
    try:
        assert isinstance(series, pd.Series)
    except AssertionError:
        raise ValueError(f'series argument must be of type pd.Series, not {type(series)}')
        
    ## Expand column
    col_expand = series.apply(pd.Series).dropna(axis=1, how='all')
    col_name = series.name

    ## Rename expanded columns to clarify source
    #add source column name prefix to each new column to identify source
    col_expand_names = [f'{col_name}_{nested_col}' for nested_col in col_expand.columns]
    #rename columns
    col_expand.columns = col_expand_names

    return col_expand

def flatten_nested_df(df):
    """Takes in a DataFrame and flattens any nested columns.
    
    Parameters
    ----------
    df : pd.DataFrame

    Returns
    -------
    pd.DataFrame
    """
    # If the df is empty, then we want to return
    if df.empty:
        return df
    # If the df is a Series, convert to DataFrame
    if isinstance(df, pd.Series):
        df = pd.DataFrame(df)
    # If there's only one column & it's not nested, return it
    if df.shape[1] == 1:
        if not is_nested(df.iloc[:,0]):
            return df
    
    # Slice the first column off of the DataFrame
    first_col, df = df.iloc[:, 0], df.iloc[:, 1:]
    
    # If the first column is nested, unnest it
    if is_nested(first_col):
        first_col = expand_series(first_col)
    
    # Want to flatten the first column again (in case nested nested) and 
    # combine with the rest of the flattened df
    return flatten_nested_df(first_col).join(flatten_nested_df(df))