import tkinter as tk
import tkinter.ttk as ttk

from pycurator.scrapers import AbstractWebScraper, WebPathScraperMixin
from pycurator.utils import button_label_frame, select_from_files

from .bases import ViewPage, idx_to_repo_selection_dict, repo_name_to_class_dict


class SelectionPage(ViewPage):
    def __init__(self, *args, **kwargs):
        ViewPage.__init__(self, *args, **kwargs)

        # Create global selection frame/widgets
        self.selector_frame = ttk.Frame(self)
        self.scraper_listbox = tk.Listbox(self.selector_frame)

        self.param_frame = ttk.Frame(self)

        self.req_var = tk.StringVar()
        self.req_label = ttk.Label(
            self.param_frame,
            foreground='#FF0000',
            textvariable=self.req_var
        )

    @ViewPage.no_overwrite
    def show(self, *args):
        """Display scraper selection widgets."""
        selection_text = ttk.Label(
            self.selector_frame,
            text='Select Repository:',
            anchor='center',
            font='helvetica 14 bold'
        )

        self.scraper_listbox.bind(
            '<<ListboxSelect>>',
            self.display_repo_params
        )

        for idx, repo_name in idx_to_repo_selection_dict.items():
            self.scraper_listbox.insert(idx, repo_name)

        # Align widgets
        selection_text.grid(
            sticky='n',
            pady=(0, 10)
        )
        self.scraper_listbox.grid(sticky='n')
        self.selector_frame.grid(row=0, column=0, sticky='n', padx=10, pady=5)
        self.param_frame.grid(row=0, column=1, sticky='n', padx=10, pady=5)
        self.grid(row=0, column=0, sticky='nsew')
        self.tkraise()

    def _set_model(self):
        # Get repository to gather params for
        repo_name = idx_to_repo_selection_dict[
            self.scraper_listbox.curselection()[0]
        ]
        repo_class = repo_name_to_class_dict[repo_name]

        self.controller.set_model(repo_class, repo_name)

    def display_repo_params(self, *args):
        """Create and display Frame with repo-specific query parameters."""
        # Clear frame
        for widget in self.param_frame.winfo_children():
            widget.destroy()

        self._set_model()

        label = ttk.Label(
            self.param_frame,
            text='Parameter Selection:',
            font='helvetica 14 bold'
        )
        label.grid(sticky='nw')

        # Initialize run button
        self.next_page_button = ttk.Button(
            self.param_frame,
            text=f'Run {self.controller.model.scraper_name}',
            command=self.controller.parse_run_parameters
        )

        # Get save location
        button_label_frame(
            root=self.param_frame,
            label_text='Save Directory:',
            button_text='Select Directory',
            button_command=lambda: select_from_files(
                root=self,
                selection_type='save_dir'
            )
        )

        # Get credentials
        if self.controller.model.scraper_class.accepts_user_credentials():
            button_label_frame(
                root=self.param_frame,
                label_text='Credentials:',
                button_text='Select File',
                button_command=lambda: select_from_files(
                    root=self,
                    selection_type='credentials',
                    filetypes=[('JSON Files', '*.json')]
                )
            )

        # Get flatten_output value
        self.controller.add_run_parameter('flatten_output', tk.IntVar())
        flatten_check = ttk.Checkbutton(
            self.param_frame,
            text='Flatten Output',
            variable=self.controller.get_run_parameter('flatten_output')
        )
        flatten_check.grid(column=0, sticky='w')

        # If repo utilizes web scraping, get path file
        if issubclass(self.controller.model.scraper_class, AbstractWebScraper):
            path_dict_frame = ttk.Frame(self.param_frame)

            # Get optional CSS path file if necessary
            if issubclass(
                    self.controller.model.scraper_class,
                    WebPathScraperMixin
            ):
                path_dict_label = ttk.Label(
                    path_dict_frame,
                    text='CSS Selector Path:'
                )
                path_dict_label.grid(row=0, column=0)

                path_dict_btn = ttk.Button(
                    path_dict_frame,
                    text='Select File',
                    command=lambda: select_from_files(
                        root=self,
                        selection_type='path_file',
                        filetypes=[('JSON Files', '*.json')]
                    )
                )
                path_dict_btn.grid(row=0, column=1)
            else:
                path_dict_btn = None

            # If web scraping is not the primary method of collection, allow
            # user to decide to scrape
            if len(self.controller.model.scraper_class.__bases__) > 1:
                self.controller.add_run_parameter('scrape', tk.IntVar())
                scrape_check_btn = ttk.Checkbutton(
                    self.param_frame,
                    text='Web Scrape',
                    variable=self.controller.get_run_parameter('scrape'),
                    state=tk.ACTIVE,
                    command=lambda: self._toggle_button_state(
                        self.controller.get_run_parameter('scrape'),
                        path_dict_btn
                    )
                )
                scrape_check_btn.grid(column=0, sticky='w')
                scrape_check_btn.invoke()

            path_dict_frame.grid(column=0, sticky='w')

        # Get search terms, if needed
        if self.controller.model.requirements.get('search_terms'):
            search_term_frame = ttk.Frame(self.param_frame)
            self.controller.add_run_parameter('search_terms', tk.StringVar())

            search_term_label = ttk.Label(
                search_term_frame,
                text='Search Terms:'
            )
            search_term_entry = ttk.Entry(
                search_term_frame,
                textvariable=self.controller.get_run_parameter('search_terms')
            )

            search_term_label.grid(row=0, column=0)
            search_term_entry.grid(row=0, column=1)
            search_term_frame.grid(columnspan=2)

        # Get search types, if needed
        if self.controller.model.requirements.get('search_types'):
            search_type_options = self.controller.model.scraper_class.search_type_options
            search_type_outer_frame = ttk.Frame(self.param_frame)
            search_type_inner_frame = ttk.Frame(search_type_outer_frame)
            search_type_label = ttk.Label(
                search_type_outer_frame,
                text='Search Types:'
            )

            self.controller.add_run_parameter(
                'search_types',
                {
                    search_type: tk.IntVar()
                    for search_type in search_type_options
                }
            )

            for search_type in search_type_options:
                search_type_button = ttk.Checkbutton(
                    search_type_inner_frame,
                    text=search_type.title(),
                    variable=self.controller.get_run_parameter('search_types')[search_type]
                )
                search_type_button.grid(sticky='w')

            search_type_label.grid(column=0, sticky='n')
            search_type_inner_frame.grid(column=1, sticky='w')
            search_type_outer_frame.grid(sticky='w')

        # Run button
        self.next_page_button.grid()

    def alert_missing_reqs(self, missing_requirements):
        try:
            self.req_label.pack_forget()
        except AttributeError:
            pass

        self.req_var.set(f'Must provide {missing_requirements} to proceed.')
        self.req_label.grid(anchor='nsew')

    def _toggle_button_state(self, toggle_vars, btn):
        if not btn:
            return
        # Validate input
        if not hasattr(toggle_vars, '__iter__'):
            toggle_vars = [toggle_vars]
        assert all([hasattr(var, 'get') for var in toggle_vars])

        # Change button state if any of the passed variables are active
        if any([var.get() for var in toggle_vars]):
            btn.config(state=tk.NORMAL)
        else:
            btn.config(state=tk.DISABLED)
