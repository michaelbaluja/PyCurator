"""
CLI for running PyCurator user interface.
"""

from pycurator.gui import interface


def main():
    """Launch Pycurator UI for collecting data from repositories."""
    root = interface.PyCuratorUI()
    root.mainloop()


if __name__ == "__main__":
    main()
