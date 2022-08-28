from pycurator.utils import saving


def test_openpyxl_save_options():
    try:
        import openpyxl
    except ImportError:
        assert 'Excel' not in saving.save_options
    else:
        assert 'Excel' in saving.save_options


def test_pyarrow_save_options():
    try:
        import pyarrow
    except ImportError:
        assert 'Parquet' not in saving.save_options \
               and 'Feather' not in saving.save_options
    else:
        assert 'Parquet' in saving.save_options \
               and 'Feather' in saving.save_options
