import pandas as pd

def is_nested(col):
    """Given an iterable, returns True if any item in the iterable is a dictionary or list"""
    return any([True for x in col if type(x) in (dict, list)])

def expand_series(series):
    """
    Given a pandas Series containing nested data, returns the nested data as a DataFrame.
    
    Params:
    - series: pd.Series
        - Series of nested data
    
    Returns:
    - col_expand: pd.DataFrame
        - DataFrame of expanded data
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
    """
    Takes in a DataFrame and flattens any nested columns.
    
    Params:
    - df: pd.DataFrame
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
    
    # Want to flatten the first column again (in case nested nested) and combine with the rest of the flattened df
    return flatten_nested_df(first_col).join(flatten_nested_df(df))