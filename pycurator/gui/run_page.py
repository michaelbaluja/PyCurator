import queue
import sys
import tkinter as tk
from tkinter.scrolledtext import ScrolledText
import tkinter.ttk as ttk

from .selection_page import SelectionPage

from .bases import ViewPage


class RunPage(ViewPage):
    def __init__(self, *args, **kwargs):
        ViewPage.__init__(self, *args, **kwargs)
        self.prev_msg = None

        # Create global runtime frames/widgets
        self.output_frame = ttk.Frame(self)
        self.runtime_output = tk.scrolledtext.ScrolledText(
            self.output_frame,
        )
        self.runtime_output.config(state='disabled')
        self.progress_bar = ttk.Progressbar(
            self.output_frame
        )

        self.progress_text_frame = ttk.Frame(self)
        self.progress_label_placeholder = ttk.Label(
            self.progress_text_frame,
            text='Querying:'
        )
        self.progress_determinate_num = ttk.Label(
            self.progress_text_frame
        )
        self.progress_label = ttk.Label(self.progress_text_frame)

        self.button_frame = ttk.Frame(self)
        self.back_button = ttk.Button(
            self.button_frame,
            text='Back',
            command=lambda: self.controller.show(SelectionPage),
            state='disabled'
        )
        self.stop_button = ttk.Button(
            self.button_frame,
            text='Stop',
            command=lambda: self.controller.request_execution()
        )
        self.exit_button = ttk.Button(
            self.button_frame,
            text='Exit',
            command=sys.exit
        )

    def _update_output(self, update, loc='end', newline=True):
        if newline:
            update = f'{update} \n'

        # Add new output
        self.runtime_output.config(state='normal')
        self.runtime_output.insert(loc, update)
        self.runtime_output.config(state='disabled')

        self.tkraise()

    @ViewPage.no_overwrite
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
        self.progress_text_frame.pack(side='bottom', fill='x')
        self.output_frame.pack(side='bottom')

        self.grid(row=0, column=0, sticky='nsew')
        self.tkraise()

        self.controller.run_scraper(self.process_updates)

    def reset_frame(self):
        # Deactivate back button
        self.back_button.config(state='disabled')

        # Replace Exit button with Stop button
        self.exit_button.pack_forget()
        self.stop_button.pack(side='left')
        self.controller.run_scraper(self.process_updates)

    def _update_progress_bar_indeterminate(self):
        self.progress_bar['mode'] = 'indeterminate'
        if self.controller.model.threaded_run.scraper.num_queries:
            self.progress_bar.start()
        else:
            self.progress_bar.stop()
            self.progress_bar['value'] = 0

    def _update_progress_bar_determinate(self):
        self.progress_bar.stop()
        self.progress_determinate_num['text'] = \
            f'({self.controller.model.threaded_run.scraper.queries_completed}/' \
            f'{self.controller.model.threaded_run.scraper.num_queries})'
        self.progress_bar['mode'] = 'determinate'
        self.progress_bar['value'] = \
            (self.controller.model.threaded_run.scraper.queries_completed /
                self.controller.model.threaded_run.scraper.num_queries * 100)

    def _update_progress_bar(self):
        """Update status of progress bar based on scraper status."""
        self.progress_label['text'] = \
            self.controller.model.threaded_run.scraper.current_query_ref

        if isinstance(self.controller.model.threaded_run.scraper.num_queries, bool):
            self._update_progress_bar_indeterminate()
        else:
            self._update_progress_bar_determinate()

    def process_updates(self):
        """Push status update from scraper queue to output."""
        # Check for updates to progress bar
        if self.controller.model.threaded_run.scraper.num_queries is not None:
            self._update_progress_bar()

        # Get next object in queue and push to output widget.
        try:
            msg = self.controller.model.threaded_run.scraper.queue.get_nowait()

            self._update_output(msg)
            self.prev_msg = msg

            # Check if process still running
            if self.controller.model.scraper.continue_running:
                self.master.after(100, self.process_updates)
            else:
                # Empty queue
                while not self.controller.model.threaded_run.scraper.queue.empty():
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
