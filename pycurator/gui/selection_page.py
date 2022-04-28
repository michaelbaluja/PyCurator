import tkinter as tk
import tkinter.ttk as ttk
from collections.abc import Iterable
from typing import Any, NoReturn

import pycurator.collectors
from pycurator import utils
from .base import ViewPage


class SelectionPage(ViewPage):
    """Selection Page of the PyCurator View component.

    Parameters
    ----------
    *args : tuple, optional
        Positional arguments for ViewPage constructor, which further
        passes to ttk.Frame.
    **kwargs : dict, optional
        Keyword arguments for ViewPage constructor, which further
        passes to ttk.Frame.

    Attributes
    ----------
    current_repository : str

    See Also
    --------
    pycurator.gui.base.ViewPage
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.current_repository = None

        # Create global selection frame/widgets
        self.selector_frame = ttk.Frame(master=self)
        self.repo_listbox = tk.Listbox(master=self.selector_frame)
        self.repo_listbox.bind(
            '<<ListboxSelect>>',
            self.display_repo_params
        )

        for repo_name in pycurator.collectors.available_repos:
            self.repo_listbox.insert(tk.END, repo_name)

        self.param_frame = ttk.Frame(master=self)

        self.req_var = tk.StringVar()
        self.req_label = ttk.Label(
            master=self.param_frame,
            foreground='#FF0000',
            textvariable=self.req_var
        )

    @ViewPage.no_overwrite
    def show(self, *args: Any) -> None:
        """Display collector selection widgets."""
        selection_text = ttk.Label(
            master=self.selector_frame,
            text='Select Repository:',
            anchor='center',
            font='helvetica 14 bold'
        )

        # Align widgets
        selection_text.grid(
            sticky='n',
            pady=(0, 10)
        )
        self.repo_listbox.grid(sticky='n')
        self.selector_frame.grid(row=0, column=0, sticky='n', padx=10, pady=5)
        self.param_frame.grid(row=0, column=1, sticky='n', padx=10, pady=5)
        self.grid(row=0, column=0, sticky='nsew')
        self.tkraise()

    def _set_model_from_name(self, repo_name) -> None:
        """Update Model element via Controller."""
        repo_class = pycurator.collectors.available_repos[repo_name]
        self.controller.set_model(repo_class, repo_name)

    def _clear_frame(self, frame: tk.Frame):
        """Delete widgets present on the provided frame."""
        for widget in frame.winfo_children():
            widget.destroy()

    def _get_selected_repo(self):
        """Get repository name from listbox selection."""
        selection_tuple = self.repo_listbox.curselection()
        if selection_tuple:
            return self.repo_listbox.get(selection_tuple[0])
        else:
            return None

    def display_repo_params(self, *args: Any) -> None:
        """Create and display Frame with repo-specific query parameters."""
        repo_selection = self._get_selected_repo()

        # No selection: nothing to show. No new selection: do not update
        if not repo_selection or repo_selection == self.current_repository:
            return

        self.current_repository = repo_selection
        self._clear_frame(self.param_frame)
        self._set_model_from_name(repo_selection)
        self.repo_listbox.selection_clear(0, tk.END)

        label = ttk.Label(
            master=self.param_frame,
            text='Parameter Selection:',
            font='helvetica 14 bold'
        )
        label.grid(sticky='nw')

        # Initialize run button
        self.next_page_button = ttk.Button(
            master=self.param_frame,
            text=f'Run {self.controller.model.collector_name}',
            command=self.controller.parse_run_parameters
        )

        # Get save information
        self.controller.add_run_parameter('save_type', tk.StringVar())
        save_dir_selection = utils.widget_label_frame(
            frame_master=self.param_frame,
            label_text='Save Directory:',
            widget_cls=ttk.Button,
            text='Select Directory',
            command=lambda: utils.select_from_files(
                root=self,
                selection_type='save_dir'
            )
        )
        save_frame = tk.Frame(master=self.param_frame)
        save_label = ttk.Label(master=save_frame, text='File Type:')
        save_type_menu = ttk.Combobox(
            master=save_frame,
            textvariable=self.controller.get_run_parameter('save_type'),
            values=[
                output_format for output_format in utils.save_options.keys()
            ],
            state='readonly'
        )
        save_label.grid(row=0, column=0)
        save_dir_selection.grid(row=0, columnspan=2, sticky='w')
        save_type_menu.grid(row=0, column=1)
        save_frame.grid()

        # Get credentials
        if self.controller.model.collector_class.accepts_user_credentials():
            user_credential_selection = utils.widget_label_frame(
                frame_master=self.param_frame,
                label_text='Credentials:',
                widget_cls=ttk.Button,
                text='Select File',
                command=lambda: utils.select_from_files(
                    root=self,
                    selection_type='credentials',
                    filetypes=[('JSON Files', '*.json')]
                )
            )
            user_credential_selection.grid(columnspan=2, sticky='w')

        # Get search terms, if needed
        if self.controller.model.requirements.get('search_terms'):
            search_term_frame = ttk.Frame(master=self.param_frame)
            self.controller.add_run_parameter('search_terms', tk.StringVar())

            search_term_label = ttk.Label(
                master=search_term_frame,
                text='Search Term(s):'
            )
            search_term_entry = ttk.Entry(
                master=search_term_frame,
                textvariable=self.controller.get_run_parameter('search_terms')
            )
            search_term_req = ttk.Label(
                master=search_term_frame,
                foreground='#FF0000',
                text='*'
            )

            search_term_req.grid(row=0, column=0, sticky='w')
            search_term_label.grid(row=0, column=1)
            search_term_entry.grid(row=0, column=2)
            search_term_frame.grid(columnspan=2)

        # Get search types, if needed
        if self.controller.model.requirements.get('search_types'):
            search_type_options = self.controller \
                .model \
                .collector_class. \
                search_type_options
            search_type_outer_frame = ttk.Frame(master=self.param_frame)
            search_type_inner_frame = ttk.Frame(master=search_type_outer_frame)
            search_type_label = ttk.Label(
                master=search_type_outer_frame,
                text='Search Type(s):'
            )
            search_type_req = ttk.Label(
                master=search_type_outer_frame,
                foreground='#FF0000',
                text='*'
            )

            self.controller.add_run_parameter(
                param='search_types',
                value={
                    search_type: tk.IntVar()
                    for search_type in search_type_options
                }
            )

            for search_type in search_type_options:
                search_type_button = ttk.Checkbutton(
                    master=search_type_inner_frame,
                    text=search_type.title(),
                    variable=self.controller.get_run_parameter(
                        'search_types'
                    )[search_type]
                )
                search_type_button.grid(sticky='w')

            search_type_req.grid(row=0, column=0, sticky='w')
            search_type_label.grid(row=0, column=1, sticky='n')
            search_type_inner_frame.grid(column=1, sticky='w')
            search_type_outer_frame.grid(sticky='w')

        # Run button
        self.next_page_button.grid()

    def alert_missing_reqs(self, missing_requirements: list[str]) -> None:
        """Display unfulfilled requirements when attempting to run."""
        try:
            self.req_label.grid_forget()
        except tk.TclError:
            pass
        finally:
            req_list = '\n'.join(
                [
                    f'\u2022 {" ".join(req.split("_")).title()}'
                    for req in missing_requirements
                ]
            )
            self.req_var.set(
                f'Missing Requirements:\n{req_list}'
            )
            self.req_label = ttk.Label(
                master=self.param_frame,
                foreground='#FF0000',
                textvariable=self.req_var
            )
        self.req_label.grid(sticky='nsew')

    def _toggle_button_state(
            self,
            toggle_vars: Iterable[tk.Variable],
            btn: tk.Button
    ) -> None:
        """Modify button state based on values of provided variables.

        Parameters
        ----------
        toggle_vars : tk.Variable or iterable of tk.Variable
            Variables on which value state is used to update
            button state.
        btn : tk.Button
            Widget to update state of.

        Raises
        ------
        TypeError
            Incorrect variable types provided to toggle_vars.
        """
        if not btn:
            return

        # Validate input
        if isinstance(toggle_vars, tk.Variable):
            toggle_vars = [toggle_vars]
        if not all([hasattr(var, 'get') for var in toggle_vars]):
            raise TypeError(
                'All entries of toggle_vars must implement "get" method.'
            )

        # Change button state if any of the passed variables are active
        if any([var.get() for var in toggle_vars]):
            btn.config(state=tk.NORMAL)
        else:
            btn.config(state=tk.DISABLED)

    def reset_frame(self) -> NoReturn:
        pass
