import utils
import pandas as pd

def test_is_nested_true():
    sample_data = [1, 2, [1, 2], {'a': 1}]
    sample_series = pd.Series(data=sample_data)

    assert utils.is_nested(sample_series)

def test_is_nested_false():
    sample_data = [1, 2, 3, 4]
    sample_series = pd.Series(data=sample_data)
    assert not utils.is_nested(sample_series)

def test_parse_numeric_string_none():
    assert utils.parse_numeric_string('there are no numbers here.') == None

def test_parse_numeric_string_individual():
    assert utils.parse_numeric_string('this string has 1 number.') == '1'

def test_parse_numeric_string_multiple():
    assert utils.parse_numeric_string('one 2 three 4') == ['2', '4']

def test_parse_numeric_string_cast():
    assert utils.parse_numeric_string('one 2 three 4', cast=True) == [2, 4]

def test_find_first_match_present():
    sample_output = utils.find_first_match(
        string='there are 213 specs of dust in this r00m',
        pattern=r'\d+'
    )

    assert sample_output == '213'