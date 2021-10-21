from tkinter import *
from scrapers import *
from utils import button_label_frame, select_file
import os

"""
ScraperGUI implementation has been adopted from Tkinter tutorial available
on geeksforgeeks.org

https://www.geeksforgeeks.org/tkinter-application-to-switch-between-different-page-frames/
"""

# Variables for scraper selection
idx_to_repo_selection_dict = {
    0: 'Dataverse',
    1: 'Dryad',
    2: 'Figshare',
    3: 'Kaggle',
    4: 'OpenML',
    5: 'Papers With Code',
    6: 'UCI',
    7: 'Zenodo'
}

repo_name_to_class_dict = {
    'Dataverse': DataverseScraper,
    'Dryad': DryadScraper,
    'Figshare': FigshareScraper,
    'Kaggle': KaggleScraper,
    'OpenML': OpenMLScraper,
    'Papers With Code': PapersWithCodeScraper,
    'UCI': UCIScraper,
    'Zenodo': ZenodoScraper
}

class ScraperGUI(Tk):
    def __init__(self, *args, **kwargs):
        Tk.__init__(self, *args, **kwargs)

        self.title('LAUC Repo Scraper')

        # Create widget window
        root = Frame(self)
        root.pack(side='top', fill='both', expand=True)

        root.grid_rowconfigure(0, weight=1)
        root.grid_columnconfigure(0, weight=1)

        # Display Selection Page
        frame = StartPage(root)
        frame.grid(row=0, column=0, sticky='nsew')
        frame.tkraise()

class StartPage(Frame):
    def __init__(self, parent):
        Frame.__init__(self, parent)
        self.parent = parent
        self.files = dict()

        # For placing repo param frames in proper grid location
        self.max_row_entries = 2

        # Show selection page
        self.show_selection_page()

    def clear_frame(self):
        for widget in self.winfo_children():
            widget.destroy()

    def show_selection_page(self):
        # Ensure frame is clear
        self.clear_frame()

        # Selection Label
        selection_text = Label(self, text='Select Scrapers')
        selection_text.grid()

        # Create Selection box with scraper names
        self.scraper_listbox = Listbox(self, selectmode='multiple')

        for idx, repo_name in idx_to_repo_selection_dict.items():
            self.scraper_listbox.insert(idx, repo_name)

        self.scraper_listbox.grid(padx=5)

        # Continue button
        continue_button = Button(
            self, 
            text='Continue', 
            command=self.show_param_page
        )
        continue_button.grid(pady=(0,5))

    def show_param_page(self):
        first_free_row_idx = 0
        # Get selection choices from selection page
        self.selection_choices = [idx_to_repo_selection_dict[idx] 
                                  for idx in self.scraper_listbox.curselection()]
        self.selection_choice_classes = [repo_name_to_class_dict[repo_name]
                                         for repo_name in self.selection_choices]
        
        self.num_search_term_repos = len(
            [issubclass(choice, (AbstractTermScraper, AbstractTermTypeScraper)) 
                for choice in self.selection_choice_classes]
        )

        self.requires_user_credentials = any(
            [repo_class.accept_user_credentials() 
             for repo_class in self.selection_choice_classes]
        )

        # Create dict to hold any files the user may upload
        self.files = {repo_name: dict() for repo_name in self.selection_choices}
        

        # Gather params for each selected repository
        self.repo_params = {choice: dict() for choice in self.selection_choices}
        self.save_vals = {choice: {'save_dataframe': IntVar()} 
                                   for choice in self.selection_choices}

        # Ensure frame is clear
        self.clear_frame()

        # Set save directory
        ## Create frame to hold save label and directory selector
        button_label_frame(
            root=self,
            label_text='Save Directory:',
            button_text='Select Directory',
            button_command=lambda: select_file(
                root=self, 
                file_type='directory'
            ),
            frame_row=first_free_row_idx,
            frame_column=0
        )
        # Increment first free row so credentials don't get covered by
        # other elements
        first_free_row_idx += 1

        # Set global search terms if applicable
        if self.num_search_term_repos > 1:
            # Create frame to hold credential label and file selector
            ## This allows the two widgets to be aligned in a single column.
            global_term_frame = Frame(self)

            search_term_label = Label(
                global_term_frame, 
                text='Global Search Terms:'
            )
            search_term_label.grid(
                row=0, 
                column=0, 
                pady=(0, 10),
                sticky='e'
            )

            self.search_terms = StringVar()
            search_term_entry = Entry(
                global_term_frame, 
                textvariable=self.search_terms
            )
            search_term_entry.grid(row=0, column=1, pady=(0, 10))

            # Align search term frame widgets together in first column
            global_term_frame.grid(
                row=first_free_row_idx, 
                column=0,
                sticky='w'
            )

            # Increment first free row so that repo param selections don't 
            # cover the search terms
            first_free_row_idx += 1
        
        # Get global credential file if applicable
        if self.requires_user_credentials:
            # Create frame to hold credential label and file selector
            ## This allows the two widgets to be aligned in a single column.
            button_label_frame(
                root=self,
                label_text='Credential File:',
                button_text='Select File',
                button_command=lambda: select_file(
                    root=self, 
                    file_type='credentials',
                    filetypes=[('Pickle Files', '*.pkl')]
                ),
                frame_row=first_free_row_idx,
                frame_column=0
            )
            # Increment first free row so credentials don't get covered by
            # other elements
            first_free_row_idx += 1

        for idx, choice in enumerate(self.selection_choices):
            row = first_free_row_idx + (idx % self.max_row_entries)
            col = idx // self.max_row_entries

            repo_class = repo_name_to_class_dict[choice]
            self._display_param_options(repo_class, choice, row, col)

        # Run button
        run_button = Button(self, text='Run Selected Scrapers', 
                            command=self.show_run_page)
        run_button.grid(sticky='nsew', pady=5)
    
    def _display_param_options(self, repo_class, repo_name, row, col):
        # Create frame
        repo_frame = Frame(self)
        repo_frame.grid(row=row, column=col, sticky='n')

        repo_label = Label(repo_frame, text=repo_name, font='helvetica 16 bold')
        repo_label.grid()

        # Get credentials for repo's that accept them
        if repo_class.accept_user_credentials():
            # Get credential data
            credentials_label = Label(repo_frame, text='API Token:')
            credentials_label.grid()
            
            self.repo_params[repo_name]['credentials'] = StringVar()
            credentials_data = Entry(
                repo_frame,
                textvariable=self.repo_params[repo_name]['credentials']
            )
            credentials_data.grid()

        # If web scraper, need to get CSS Selector Path
        if issubclass(repo_class, AbstractWebScraper):
            path_file_label = Label(repo_frame, text='CSS Selectors Path File:')
            path_file_label.grid()

            path_file_button = Button(
                repo_frame, 
                text='Choose File', 
                command=lambda: select_file(
                    root=self, 
                    file_type='path_file',
                    repo_name=repo_name,
                    filetypes=[('JSON Files', '*.json')]
                )
            )
            path_file_button.grid()

        # Get save values
        save_label = Label(repo_frame, text='Save Data:')
        save_label.grid(sticky='w')
        save_df = Checkbutton(
            repo_frame, 
            text='Save DataFrames', 
            variable=self.save_vals[repo_name]['save_dataframe']
        )
        save_df.grid(sticky='w')

        # Get flatten_output value
        self.repo_params[repo_name]['flatten_output'] = IntVar()
        flatten_check = Checkbutton(
                repo_frame, 
                text='Flatten Output', 
                variable=self.repo_params[repo_name]['flatten_output']
            )
        flatten_check.grid(sticky='w')

        # Get search terms if needed
        if issubclass(repo_class, (AbstractTermScraper, AbstractTermTypeScraper)):
            search_term_label = Label(repo_frame, text='Search Terms:')
            search_term_label.grid()

            search_terms = StringVar()

            search_term_entry = Entry(repo_frame, textvariable=search_terms)
            search_term_entry.grid()

            self.repo_params[repo_name]['search_terms'] = search_terms
        
        # Get search types if needed
        if issubclass(repo_class, (AbstractTypeScraper, AbstractTermTypeScraper)):
            search_type_label = Label(repo_frame, text='Search Types:')
            search_type_label.grid(sticky='w')

            self.repo_params[repo_name]['search_types'] = \
                {search_type: IntVar() 
                 for search_type in repo_class.search_type_options}

            for search_type in repo_class.search_type_options:
                search_type_checkbutton = Checkbutton(
                    repo_frame, 
                    text=search_type.title(),
                    variable=self.repo_params[repo_name]['search_types'][search_type]
                )
                search_type_checkbutton.grid(sticky='w')
            

    def _evaluate_selection_values(self):
        # Parse run options
        for repo_name, repo_class in zip(self.selection_choices, 
                                         self.selection_choice_classes):
            if repo_class.accept_user_credentials():
                # Get API Token parameter
                credentials = self.repo_params[repo_name]['credentials'].get()

                # If no token was entered, default to selected credential file
                # If no file was selected, then set null credentials
                if not credentials:
                    try:
                        credentials = self.files['credentials']
                    except KeyError:
                        credentials = ''
                
                self.repo_params[repo_name]['credentials'] = credentials

            if issubclass(repo_class, AbstractWebScraper):
                # Get path_file paramter
                self.repo_params[repo_name]['path_file'] = \
                    self.files[repo_name]['path_file']

            # Get flatten_output parameter
            self.repo_params[repo_name]['flatten_output'] = \
                self.repo_params[repo_name]['flatten_output'].get()

            # Get save parameters
            self.save_vals[repo_name] = \
                {key: val.get() for key, val in self.save_vals[repo_name].items()}
            if not hasattr(self, 'base_save_dir'):
                self.base_save_dir = 'data'
            
        # Add global search terms to each repo if specific terms were not provided
        for repo_name in self.selection_choices:
            local_search_terms = self.repo_params[repo_name].get('search_terms')
            try:
                if local_search_terms.get() ==  '':
                    self.repo_params[repo_name]['search_terms'] = self.search_terms
            except AttributeError:
                pass

        # Parse search terms
        for repo_name in self.selection_choices:
            local_search_terms = self.repo_params[repo_name].get('search_terms')
            if local_search_terms:
                self.repo_params[repo_name]['search_terms'] = \
                    local_search_terms.get().split(',')

        # Parse search types
        for repo_name in self.selection_choices:
            local_search_types = self.repo_params[repo_name].get('search_types')
            if local_search_types:
                self.repo_params[repo_name]['search_types'] = \
                    [search_type for search_type, val in local_search_types.items()
                     if val.get()]

    def show_run_page(self):
        # Get selection values
        self._evaluate_selection_values()

        for repo_name in self.selection_choices:
            # Ensure frame is clear
            self.clear_frame()

            repo_class = repo_name_to_class_dict[repo_name]

            run_label = Label(self, text=f'Running {repo_name}...')
            run_label.grid()

            try:
                print(f'Running {repo_name}...')

                # Create scraper object and run scraping functionality
                scraper = repo_class(**self.repo_params[repo_name])
                scraper.run(
                    **self.save_vals[repo_name], 
                    save_dir=os.path.join(self.base_save_dir, repo_name)
                )

                print(f'{repo_name} run complete.')
            except Exception as e:
                print('\n', f'Error: {e}')
                error = Label(self, text=e)
                error.grid()
                raise(e)
            else:
                run_complete_label = Label(self, text=f'{repo_name} completed.')
                run_complete_label.grid()

        # Quit button
        quit_button = Button(self, text='Exit', command=quit)
        quit_button.grid()


if __name__ == '__main__':
    app = ScraperGUI()
    app.mainloop()
