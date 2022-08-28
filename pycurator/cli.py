"""
CLI for running PyCurator user interface.
"""

from .gui import PyCuratorUI


def main():
    root = PyCuratorUI()
    root.mainloop()


if __name__ == "__main__":
    main()
