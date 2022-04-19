from pycurator import utils


def test_openpyxl_save_options():
    try:
        import openpyxl
    except ImportError:
        assert 'Excel' not in utils.save_options
    else:
        assert 'Excel' in utils.save_options


def test_pyarrow_save_options():
    try:
        import pyarrow
    except ImportError:
        assert 'Parquet' not in utils.save_options \
               and 'Feather' not in utils.save_options
    else:
        assert 'Parquet' in utils.save_options \
               and 'Feather' in utils.save_options
