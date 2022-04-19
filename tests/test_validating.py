import pytest

from pycurator import utils
from .fixtures import (
    metadata_params,
    sample_args,
    sample_kwargs,
    type_collector
)


def test_validate_save_filename():
    assert utils.validate_save_filename(
        '\'output\' "file".txt'
    ) == 'output_file.txt'


def test_validate_metadata_parameters_single(metadata_params):
    assert utils.validate_metadata_parameters(
        metadata_params[0]
    ) == [metadata_params[0]]


def test_validate_metadata_parameters_multiple(metadata_params):
    assert utils.validate_metadata_parameters(
        metadata_params
    ) == metadata_params


def test_validate_metadata_parameters_error(metadata_params):
    with pytest.raises(TypeError):
        utils.validate_metadata_parameters(list(map(int, metadata_params)))


def test_validate_from_arguments_no_params(type_collector):
    with pytest.raises(ValueError):
        utils.validate_from_arguments(
            base=type_collector,
            param='search_type',
            func=type_collector.get_query_metadata
        )


def test_validate_from_arguments_args(sample_args, type_collector):
    assert utils.validate_from_arguments(
        base=type_collector,
        param='search_type',
        func=type_collector.get_query_metadata,
        args=sample_args,
    ) == (sample_args, dict())


def test_validate_from_arguments_kwargs(sample_kwargs, type_collector):
    assert utils.validate_from_arguments(
        base=type_collector,
        param='search_type',
        func=type_collector.get_query_metadata,
        kwargs=sample_kwargs,
    ) == (set(), sample_kwargs)


def test_validate_from_arguments_args_kwargs(
        sample_args,
        sample_kwargs,
        type_collector
):
    assert utils.validate_from_arguments(
        base=type_collector,
        param='search_type',
        func=type_collector.get_query_metadata,
        args=sample_args,
        kwargs=sample_kwargs
    ) == (sample_args, sample_kwargs)
