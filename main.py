from gui import ScraperGUI
import tkinter as tk

if __name__ == '__main__':
    root = tk.Tk()
    root.title('PyCurator')
    main = ScraperGUI(root)
    main.pack(side='top', fill='both', expand=True)
    root.wm_geometry('550x390')
    root.mainloop()