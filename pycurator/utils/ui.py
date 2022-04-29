from __future__ import annotations

import tkinter as tk
import tkinter.ttk as ttk
from typing import ParamSpec, Type, TypeVar


from pycurator import gui

P = ParamSpec('P')
T = TypeVar('T')


def widget_label_frame(
        frame_master: tk.Misc,
        label_text: float | str,
        widget_cls: Type[tk.Widget],
        **widget_kwargs: P.kwargs
) -> ttk.Frame:
    """Create frame containing vertically-aligned widget and label.

    Parameters
    ----------
    frame_master : tk.Misc
    label_text : float or str
    widget_cls : tk.Widget
    **widget_kwargs

    Returns
    -------
    _frame : ttk.Frame

    Examples
    --------
    >>> root = tk.Tk()
    >>> widget_label_frame(
        frame_master=root,
        label_text="Don't click this button:",
        widget_cls=ttk.Button,
        widget_text='Click me!',
        widget_command=sys.exit
    )
    """

    _frame = ttk.Frame(master=frame_master)
    _label = ttk.Label(master=_frame, text=label_text)
    _widget = widget_cls(
        master=_frame,
        **widget_kwargs
    )

    _label.grid(row=0, column=0)
    _widget.grid(row=0, column=1)
    return _frame


def select_from_files(
        root: gui.ViewPage,
        selection_type: str,
        filetypes: list[tuple[str, str]] = (('All File Types', '*.*'),)
) -> None:
    """Allows user to select local file/directory.

    Parameters
    ----------
    root : Tkinter.Frame
        Derived Frame class that contains UI.
        If choosing file, frame_master must contain "files" dict attribute to
        hold the selected file.
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
        Occurs when called on a frame_master that does not contain a files
        attribute.

    Examples
    --------
    >>> f = tk.Frame()
    >>> select_from_files(
    ...     root=f,
    ...     selection_type='dir'
    ... )
    Traceback (most recent call last):
        ...
    NotImplementedError: 'root' must include add_run_parameter() method.'

    >>> from pycurator.gui import ViewPage
    >>> view = ViewPage()
    >>> select_from_files(
    ...     root=view,
    ...     selection_type='dir',
    ... )
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
            '\'root\' must include add_run_parameter() method.'
        )
