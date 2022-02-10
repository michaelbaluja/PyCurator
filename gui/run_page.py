import os
import queue
import sys
import tkinter as tk
from tkinter.scrolledtext import ScrolledText
import tkinter.ttk as ttk

from scrapers import AbstractScraper

from .bases import Page, ThreadedRun


class RunPage(Page):
    def __init__(self, *args, **kwargs):
        Page.__init__(self, *args, **kwargs)
        self.prev_msg = None

    def evaluate_parameters(self, repo_name):
        """Transform provided tkinter variables into Python values.

        Parameters
        ----------
        repo_name : str
            Name of the repository to evaluate

        Returns
        -------
        param_kwargs : dict
            Dictionary of evaluated parameters in format to pass to class init.
        """

        # Parse run options
        # Handle search_types, save_dir separately since they're not tkinter
        # variables
        try:
            search_types = self.master.repo_params[repo_name].pop(
                'search_types'
            )
            search_types = [
                search_type
                for search_type, val in search_types.items() if val.get()
            ]
        except KeyError:
            search_types = None

        try:
            save_dir = self.master.repo_params[repo_name].pop('save_dir')
        except KeyError:
            # Add default directory if not is specified
            save_dir = os.path.join('data', repo_name)
            if not os.path.isdir(save_dir):
                os.makedirs(save_dir, exist_ok=True)

        param_kwargs = {
            param: val.get()
            for param, val in self.master.repo_params[repo_name].items()
        }

        # Split search_terms if multiple
        if param_kwargs.get('search_terms'):
            param_kwargs['search_terms'] = \
                param_kwargs['search_terms'].split(', ')

        # Add previously removed variables back
        param_kwargs['save_dir'] = save_dir
        if search_types:
            param_kwargs['search_types'] = search_types

        return param_kwargs

    def run(self, repo_name, repo_class):
        """Create scraper object and set up display functionality.

        Parameters
        ----------
        repo_name : str
        repo_class
            Class for repository. Must be derived from AbstractScraper
        """

        assert isinstance(repo_name, str)
        assert issubclass(repo_class, AbstractScraper)

        self.repo_name = repo_name
        self.repo_class = repo_class

        # Evaluate parameter values
        param_kwargs = self.evaluate_parameters(repo_name)
        save_dir = param_kwargs.pop('save_dir')

        # Create instance of scraper
        self.scraper = repo_class(**param_kwargs)
        self.threaded_run = ThreadedRun(
            scraper=self.scraper,
            kwargs={'save_dir': save_dir}
        )

        self.show()

    def _update_output(self, update, loc='end', newline=True):
        if newline:
            update = f'{update} \n'

        # Add new output
        self.runtime_output.config(state='normal')
        self.runtime_output.insert(loc, update)
        self.runtime_output.config(state='disabled')

        super().show()

    def show(self):
        """Display runtime status updates and navigation buttons.

        Organizes the elements of the RunPage display. The output contains a
        ScrolledText widget for displaying ThreadedRun scraper updates, a
        ProgressBar widget for updates on loop events, and Button widgets for
        paging back after completion, stopping the scraper object during a
        run, and exiting the application upon scraper termination/completion.

        Notes
        -----
        The ScrolledText widget updates variably using the self.process_queue
        function to retrieve runtime updates from the ThreadedRun scraper
        object and self._update_output to push the updates to the display.

        The ProgressBar is used for loop queries in the scraper object in order
        to provide continual updates to the user without pushing updates to
        the ScrolledText widget for every query made, as many of the APIs being
        querying may return hundreds to thousands of results to query. The
        indeterminate mode is used for repository queries performed in a while
        loop (such as Dryad and Zenodo), while determinate mode is used for
        repository queries over a fixed list of queries in a for loop
        (such as UCI searching over scraped pages).

        The navigation Buttons are set up in order to provide different
        functionality based on scraper status. During runtime, the back button
        is disabled, and then reactivated upon scraper completion. The end/exit
        button is set to stop the scraper if pressed during runtime, and set to
        exit the program if pressed after scraper completion.
        """

        # Create layout frames
        self.run_frame = tk.Frame()
        self.output_frame = tk.Frame(self.run_frame)
        self.progress_text_frame = tk.Frame(self.run_frame)
        self.output_frame2 = tk.Frame(self.run_frame)
        self.button_frame = tk.Frame(self.run_frame)

        # Add update output
        self.runtime_output = tk.scrolledtext.ScrolledText(
            self.output_frame,
        )
        self.runtime_output.config(state='disabled')

        # Add progress indicator
        self.progress_label_placeholder = tk.Label(
            self.progress_text_frame,
            text='Querying:'
        )
        self.progress_determinate_num = tk.Label(
            self.progress_text_frame
        )
        self.progress_label = tk.Label(self.progress_text_frame)
        self.progress_bar = ttk.Progressbar(
            self.output_frame2
        )

        # Add navigation buttons
        self.back_button = ttk.Button(
            self.button_frame,
            text='Back',
            command=self.hide,
            state='disabled'
        )
        self.stop_button = ttk.Button(
            self.button_frame,
            text='Stop',
            command=self.scraper.request_execution
        )
        self.exit_button = ttk.Button(
            self.button_frame,
            text='Exit',
            command=sys.exit
        )

        # Arrange elements
        self.runtime_output.pack(side='top', expand=True, fill='both')

        self.progress_label_placeholder.pack(side='left', anchor='w')
        self.progress_determinate_num.pack(side='left', anchor='w')
        self.progress_label.pack(side='left', fill='x')
        self.progress_bar.pack(
            side='bottom',
            anchor='s',
            fill='x',
            expand=True
        )

        self.back_button.pack(side='left', fill='both')
        self.stop_button.pack(side='left', fill='both')

        self.button_frame.pack(side='bottom', expand=True)
        self.output_frame2.pack(side='bottom', fill='x')
        self.progress_text_frame.pack(side='bottom', fill='x')
        self.output_frame.pack(side='bottom')

        self.run_frame.place(in_=self)

        # Start run
        self.threaded_run.start()
        self.master.after(100, self.process_updates)

        super().show()

    def _update_progress_bar_indeterminate(self):
        self.progress_bar['mode'] = 'indeterminate'
        if self.threaded_run.scraper.num_queries:
            self.progress_bar.start()
        else:
            self.progress_bar.stop()
            self.progress_bar['value'] = 0

    def _update_progress_bar_determinate(self):
        self.progress_bar.stop()
        self.progress_determinate_num['text'] = \
            f'({self.threaded_run.scraper.queries_completed}/' \
            f'{self.threaded_run.scraper.num_queries})'
        self.progress_bar['mode'] = 'determinate'
        self.progress_bar['value'] = \
            (self.threaded_run.scraper.queries_completed /
                self.threaded_run.scraper.num_queries * 100)

    def _update_progress_bar(self):
        """Update status of progress bar based on scraper status."""
        self.progress_label['text'] = \
            self.threaded_run.scraper.current_query_ref

        if isinstance(self.threaded_run.scraper.num_queries, bool):
            self._update_progress_bar_indeterminate()
        else:
            self._update_progress_bar_determinate()

    def process_updates(self):
        """Push status update from scraper queue to output."""
        # Check for updates to progress bar
        if self.threaded_run.scraper.num_queries is not None:
            self._update_progress_bar()

        # Get next object in queue and push to output widget.
        try:
            msg = self.threaded_run.scraper.queue.get_nowait()

            self._update_output(msg)
            self.prev_msg = msg

            # Check if process still running
            if self.scraper.continue_running:
                self.master.after(100, self.process_updates)
            else:
                # Empty queue
                while not self.threaded_run.scraper.queue.empty():
                    self.process_updates()

                # Stop progress bar
                self.progress_bar.stop()

                # Reactivate back button
                self.back_button.config(state='normal')

                # Replace Stop button with Exit button
                self.stop_button.pack_forget()
                self.exit_button.pack(side='left')

                return
        # If the queue is empty, continually check
        except queue.Empty:
            self.master.after(100, self.process_updates)

    def hide(self):
        """Remove frame from view and display the previous (selection) page."""
        self.run_frame.place_forget()
        self.master.selection_page.show()
