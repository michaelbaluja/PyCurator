from __future__ import annotations

import os
import tkinter as tk
import tkinter.ttk as ttk
from typing import Any, TypeVar

import pandas as pd

import pycurator.gui.bases
from .parsing import _validate_save_filename

T = TypeVar('T')


def save_results(results: dict, data_dir: str, extension: str) -> None:
    """Export DataFrame objects to json file in specified directory.

    Parameters
    ----------
    results : dict
    data_dir : str
    extension : {'csv', 'json'}

    Raises
    ------
    TypeError
        "results" not of type dict or "datadir" not of type str.
    """

    if not isinstance(results, dict):
        raise TypeError(
            f'results must be of type dict, not \'{type(results)}\'.'
        )
    if not isinstance(data_dir, str):
        raise TypeError(
            f'data_dir must of type str, not \'{type(data_dir)}\'.'
        )
    if extension not in ('csv', 'json'):
        raise ValueError(
            f'extension must be either csv or json, not \'{extension}\'.'
        )

    if not os.path.isdir(data_dir):
        os.makedirs(data_dir)

    for query, df in results.items():
        if isinstance(query, str):
            output_filename = f'{query}.{extension}'
        else:
            search_term, search_type = query
            output_filename = f'{search_term}_{search_type}.{extension}'

        output_filename = _validate_save_filename(output_filename)

        save_dataframe(
            results=df,
            filepath=os.path.join(data_dir, output_filename)
        )


def save_dataframe(results: pd.DataFrame, filepath: str) -> None:
    """Saves the specified results to the file provided.

    Parameters
    ----------
    results : pandas.DataFrame
        If DataFrame, results will be stored in a csv format.
    filepath : str
        Location to store file in. Take note of output type as specified
        above, as appending the incorrect filetype may result in the file
        being unreadable.

    Raises
    ------
    ValueError
        If a non-dataframe object is passed.
    """

    if not isinstance(results, pd.DataFrame):
        raise ValueError(
            f'Input must be of type pandas.DataFrame, not'
            f' \'{type(results)}\'.'
        )

    if filepath.endswith('csv'):
        results.to_csv(filepath)
    elif filepath.endswith('json'):
        results.to_json(filepath)
    else:
        raise ValueError(
            f'\'{filepath}\' is not supported.'
            'Data can only be saved to json or csv.'
        )


def button_label_frame(
        root: tk.Misc,
        label_text: float | str,
        button_text: float | str,
        button_command: Any
) -> None:
    _frame = ttk.Frame(root)
    _label = ttk.Label(_frame, text=label_text)
    _button = ttk.Button(_frame, text=button_text, command=button_command)

    _label.grid(row=0, column=0)
    _button.grid(row=0, column=1)
    _frame.grid(sticky='w', columnspan=2)


def select_from_files(
        root: pycurator.gui.bases.ViewPage,
        selection_type: str,
        filetypes: list[tuple[str, str]] = (('All File Types', '*.*'),)
) -> None:
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
    filetypes : list of two-tuples of str,
            optional (default = [('All File Types', '*.*')])
        Allows users to input filetype restrictions.
        Each tuple should be of the form:
            ('File Type', '{file_name}.{file_format}')

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
