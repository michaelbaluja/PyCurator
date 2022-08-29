"""
CLI for running PyCurator user interface.
"""

from . import gui


def main():
    root = gui.interface.PyCuratorUI()
    root.mainloop()


if __name__ == "__main__":
    main()
