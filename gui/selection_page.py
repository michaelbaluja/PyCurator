import tkinter as tk

from scrapers import (AbstractTermScraper, AbstractTermTypeScraper,
                      AbstractTypeScraper, AbstractWebScraper)
from utils import button_label_frame, select_from_files

from .bases import Page, idx_to_repo_selection_dict, repo_name_to_class_dict


class SelectionPage(Page):
    def __init__(self, *args, **kwargs):
        Page.__init__(self, *args, **kwargs)

    def show(self):
        """Display scraper selection widgets."""
        # Create Frames to divide page into selection/param sections
        # Frames are created as class vars so they can be updated from other
        # functions, as is necessary for the param frame
        self.selection_page_frame = tk.Frame()
        self.selector_frame = tk.Frame(self.selection_page_frame)
        self.param_frame = tk.Frame(self.selection_page_frame)
        self.param_frame.files = dict()

        # Create Selection Frame
        # Selection Label
        selection_text = tk.Label(
            self.selector_frame,
            text='Select Repository:',
            anchor='center',
            font='helvetica 14 bold'
        )
        selection_text.pack(
            side='top', 
            pady=(0, 10), 
            anchor='w'
        )

        # Create Selection box with Scraper names
        self.scraper_listbox = tk.Listbox(self.selector_frame)
        self.scraper_listbox.bind('<<ListboxSelect>>', self.show_repo_params)

        for idx, repo_name in idx_to_repo_selection_dict.items():
            self.scraper_listbox.insert(idx, repo_name)

        self.scraper_listbox.pack(side='top', expand=True, anchor='n')

        # Align Frames in window
        self.selector_frame.pack(
            side='left',
            anchor='n',
            fill='both',
            expand=True,
            padx=(30, 0),
            pady=(30, 0)
        )
        self.param_frame.pack(
            side='left',
            anchor='n',
            fill='both',
            expand=True,
            padx=(30, 0),
            pady=(30, 0)
        )
        self.selection_page_frame.place(in_=self)

        super().show()

    def show_repo_params(self, *args):
        """Create and display Frame with repo-specific query parameters."""
        # Clear frame
        for widget in self.param_frame.winfo_children():
            widget.destroy()

        label = tk.Label(
            self.param_frame, 
            text='Parameter Selection:',
            font='helvetica 14 bold'
        )
        label.pack(side='top', pady=(0, 10), anchor='w')

        # Get repository to gather params for
        repo_name = idx_to_repo_selection_dict[
            self.scraper_listbox.curselection()[0]
        ]
        repo_class = repo_name_to_class_dict[repo_name]
        self.master.repo_params[repo_name] = dict()

        # Initialize run button (must be before search_type initialization)
        self.run_button = tk.Button(
            self.param_frame,
            text=f'Run {repo_name}',
            command=lambda: self.validate_and_run(repo_name, repo_class)
        )

        # Get save location
        button_label_frame(
            root=self.param_frame,
            label_text='Save Directory:',
            button_text='Select Directory',
            button_command=lambda: select_from_files(
                root=self.param_frame,
                selection_type='save_dir'
            )
        )

        # Get flatten_output value
        self.master.repo_params[repo_name]['flatten_output'] = tk.IntVar()
        flatten_check = tk.Checkbutton(
            self.param_frame,
            text='Flatten Output',
            variable=self.master.repo_params[repo_name]
        )
        flatten_check.pack(anchor='w')

        # If repo utilizes web scraping, get path file
        if issubclass(repo_class, AbstractWebScraper):
            path_dict_frame = tk.Frame(self.param_frame)
            path_dict_label = tk.Label(
                path_dict_frame,
                text='CSS Selector Path:'
            )
            path_dict_label.pack(side='left')

            self.path_dict_btn = tk.Button(
                path_dict_frame,
                text='Choose File',
                command=lambda: select_from_files(
                    root=self.param_frame,
                    selection_type='path_file',
                    filetypes=[('JSON Files', '*.json')]
                )
            )
            self.path_dict_btn.pack(side='right')

            # If web scraping is not the primary method of collection, allow
            # user to decide to scrape
            if len(repo_class.__bases__) > 1:
                self.master.repo_params[repo_name]['scrape'] = tk.IntVar()
                scrape_check_btn = tk.Checkbutton(
                    self.param_frame,
                    text='Web Scrape',
                    variable=self.master.repo_params[repo_name]['scrape'],
                    state=tk.ACTIVE,
                    command=lambda: self._toggle_button_state(
                        self.master.repo_params[repo_name]['scrape'], 
                        self.path_dict_btn
                    )
                )
                scrape_check_btn.pack(anchor='w')
                scrape_check_btn.select()

            path_dict_frame.pack(anchor='w')

        # Get search terms, if needed
        if hasattr(repo_class, 'set_search_terms'):
            search_term_frame = tk.Frame(self.param_frame)
            self.master.repo_params[repo_name]['search_terms'] = tk.StringVar()

            search_term_label = tk.Label(
                search_term_frame,
                text='Search Terms:'
            )
            search_term_entry = tk.Entry(
                search_term_frame,
                textvariable=self.master.repo_params[repo_name]['search_terms']
            )

            search_term_label.pack(side='left')
            search_term_entry.pack(side='right')
            search_term_frame.pack()

        # Get search types, if needed
        if hasattr(repo_class, 'set_search_types'):
            search_type_outer_frame = tk.Frame(self.param_frame)
            search_type_inner_frame = tk.Frame(search_type_outer_frame)
            search_type_label = tk.Label(
                search_type_outer_frame,
                text='Search Types:'
            )

            self.master.repo_params[repo_name]['search_types'] = {
                search_type: tk.IntVar()
                for search_type in repo_class.get_search_type_options()
            }

            for search_type in repo_class.get_search_type_options():
                search_type_button = tk.Checkbutton(
                    search_type_inner_frame,
                    text=search_type.title(),
                    variable=self.master.repo_params[repo_name] \
                        ['search_types'][search_type]
                    )
                search_type_button.pack(side='top', anchor='w')

            search_type_label.pack(side='left', anchor='n')
            search_type_inner_frame.pack(side='right')
            search_type_outer_frame.pack(anchor='w')

        # Run button
        self.run_button.pack(side='bottom', anchor='center', pady=(5, 0))

    def validate_and_run(self, repo_name, repo_class):
        """Ensure all required parameters are entered before running.

        Parameters
        ----------
        repo_name : str
        repo_class : AbstractScraper derivative

        See Also
        --------
        scrapers.base_scrapers
        """

        requirements_left = []
        search_terms = self.master.repo_params[repo_name].get('search_terms')
        search_types = self.master.repo_params[repo_name].get('search_types')

        if search_terms and not search_terms.get():
            requirements_left.append('search term(s)')
        if (search_types and 
            not any([type_.get() for type_ in search_types.values()])):
            requirements_left.append('search type(s)')

        # Remove any previous requirement label
        try:
            self.requirement_label.pack_forget()
        except:
            pass

        if requirements_left:
            self.requirement_label = tk.Label(
                self.param_frame, 
                fg='#FF0000',
                text=f'Must provide {requirements_left} to proceed.'
            )
            self.requirement_label.pack(anchor='center')
        else:
            self.master.run_page.run(repo_name, repo_class)

    def _toggle_button_state(self, toggle_vars, btn):
        # Validate input
        if not hasattr(toggle_vars, '__iter__'):
            toggle_vars = [toggle_vars]
        assert all([hasattr(var, 'get') for var in toggle_vars])
        
        # Change button state if any of the passed variables are active
        if any([var.get() for var in toggle_vars]):
            btn.config(state=tk.NORMAL)
        else:
            btn.config(state=tk.DISABLED)
