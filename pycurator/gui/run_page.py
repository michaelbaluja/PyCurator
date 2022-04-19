import sys
import tkinter as tk
import tkinter.ttk as ttk
from tkinter.scrolledtext import ScrolledText
from typing import Any

from .base import ViewPage
from .selection_page import SelectionPage


class RunPage(ViewPage):
    """Run Page of the PyCurator View component.

    Parameters
    ----------
    *args : tuple, optional
        Positional arguments for ViewPage constructor, which further
        passes to ttk.Frame.
    **kwargs : dict, optional
        Keyword arguments for ViewPage constructor, which further
        passes to ttk.Frame.

    See Also
    --------
    pycurator.gui.base.ViewPage
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.prev_msg = None

        # Create global runtime frames/widgets
        self.output_frame = ttk.Frame(self)
        self.runtime_output = tk.scrolledtext.ScrolledText(
            master=self.output_frame,
        )
        self.runtime_output.config(state='disabled')
        self.progress_bar = ttk.Progressbar(master=self.output_frame)

        self.progress_text_frame = ttk.Frame(master=self)
        self.progress_label_placeholder = ttk.Label(
            master=self.progress_text_frame,
            text='Querying:'
        )
        self.progress_determinate_num = ttk.Label(
            master=self.progress_text_frame
        )
        self.progress_label = ttk.Label(master=self.progress_text_frame)

        self.button_frame = ttk.Frame(master=self)
        self.back_button = ttk.Button(
            master=self.button_frame,
            text='Back',
            command=lambda: self.controller.show(SelectionPage),
            state='disabled'
        )
        self.stop_button = ttk.Button(
            master=self.button_frame,
            text='Stop',
            command=lambda: self.controller.request_execution()
        )
        self.exit_button = ttk.Button(
            master=self.button_frame,
            text='Exit',
            command=sys.exit
        )

    @ViewPage.no_overwrite
    def show(self) -> None:
        """Display runtime status updates and navigation buttons.

        Organizes the elements of the RunPage display. The output contains a
        ScrolledText widget for displaying ThreadedRun collector updates, a
        ProgressBar widget for updates on loop events, and Button widgets for
        paging back after completion, stopping the collector object during a
        run, and exiting the application upon collector termination/completion.

        Notes
        -----
        The ScrolledText widget updates variably using the self.process_queue
        function to retrieve runtime updates from the ThreadedRun collector
        object and self._update_output to push the updates to the display.

        The ProgressBar is used for loop queries in the collector object in
        order to provide continual updates to the user without pushing updates
        to the ScrolledText widget for every query made, as many of the APIs
        being querying may return hundreds to thousands of results to query.
        The indeterminate mode is used for repository queries performed in a
        while loop (such as Dryad and Zenodo), while determinate mode is used
        for repository queries over a fixed list of queries in a for loop
        (such as UCI searching over scraped pages).

        The navigation Buttons are set up in order to provide different
        functionality based on collector status. During runtime, the back
        button is disabled, and then reactivated upon collector completion. The
        end/exit button is set to stop the collector if pressed during runtime,
        and set to exit the program if pressed after collector completion.
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

        self.controller.run_collector()

    def reset_frame(self) -> None:
        """Modify frame widgets for reuse."""
        # Deactivate back button
        self.back_button.config(state='disabled')

        # Replace Exit button with Stop button
        self.exit_button.pack_forget()
        self.stop_button.pack(side='left')
        self.controller.run_collector()
