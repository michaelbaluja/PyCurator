import tkinter as tk

from .bases import Page


class LandingPage(Page):
    def __init__(self, *args, **kwargs):
        Page.__init__(self, *args, **kwargs)

    def show(self):
        # Landing page information
        label = tk.Label(
            self, 
            text='PyCurator', 
            font='helvetica 16 bold'
        )

        # Load landing page text
        with open('gui/landing_msg.txt') as f:
            message = tk.StringVar(value=f.read())
        
        message_box = tk.Message(self, textvariable=message)

        continue_button = tk.Button(
            self, 
            text='Continue', 
            command=self.master.selection_page.show
        )

        # Arrange elements
        label.pack(side='top', expand=True)
        message_box.pack(side='top', anchor='n', expand=True)
        continue_button.pack(side='top', expand=True, pady=(0, 10))

        super().show()
