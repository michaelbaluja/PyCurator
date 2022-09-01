"""
CLI for running PyCurator user interface.
"""

from . import gui


def main():
    """Launch Pycurator UI for collecting data from repositories."""
    root = gui.interface.PyCuratorUI()
    root.mainloop()


if __name__ == "__main__":
    main()
