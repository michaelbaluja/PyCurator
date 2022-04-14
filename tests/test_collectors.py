#
#  PyCurator LGPL 3.0 <https://www.gnu.org/licenses/lgpl-3.0.txt>
#  Copyright (c) 2022. Michael Baluja
#
#  This file is part of PyCurator.
#  PyCurator is free software: you can redistribute it and/or modify it under
#  the terms of version 3 of the GNU Lesser General Public License as published
#  by the Free Software Foundation.
#  PyCurator is distributed in the hope that it will be useful, but WITHOUT ANY
#  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
#  FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
#  details. You should have received a copy of the GNU Lesser General Public
#  License along with PyCurator. If not, see <https://www.gnu.org/licenses/>.

#
#
#  This file is part of PyCurator.
#  PyCurator is free software: you can redistribute it and/or modify it under
#  the terms of version 3 of the GNU Lesser General Public License as published
#  by the Free Software Foundation.
#  PyCurator is distributed in the hope that it will be useful, but WITHOUT ANY
#  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
#  FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
#  details. You should have received a copy of the GNU Lesser General Public
#  License along with PyCurator. If not, see <https://www.gnu.org/licenses/>.

#
#
#  This file is part of PyCurator.
#  PyCurator is free software: you can redistribute it and/or modify it under
#  the terms of version 3 of the GNU Lesser General Public License as published
#  by the Free Software Foundation.
#  PyCurator is distributed in the hope that it will be useful, but WITHOUT ANY
#  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
#  FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
#  details. You should have received a copy of the GNU Lesser General Public
#  License along with PyCurator. If not, see <https://www.gnu.org/licenses/>.

#
#
#  This file is part of PyCurator.
#  PyCurator is free software: you can redistribute it and/or modify it under
#  the terms of version 3 of the GNU Lesser General Public License as published
#  by the Free Software Foundation.
#  PyCurator is distributed in the hope that it will be useful, but WITHOUT ANY
#  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
#  FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
#  details. You should have received a copy of the GNU Lesser General Public
#  License along with PyCurator. If not, see <https://www.gnu.org/licenses/>.

#
#
#  This file is part of PyCurator.
#  PyCurator is free software: you can redistribute it and/or modify it under
#  the terms of version 3 of the GNU Lesser General Public License as published
#  by the Free Software Foundation.
#  PyCurator is distributed in the hope that it will be useful, but WITHOUT ANY
#  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
#  FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
#  details. You should have received a copy of the GNU Lesser General Public
#  License along with PyCurator. If not, see <https://www.gnu.org/licenses/>.

import pandas as pd
import pytest

from pycurator import collectors


def test_kaggle_availability():
    try:
        import kaggle
    except (ImportError, OSError):
        assert 'Kaggle' not in collectors.available_repos
    else:
        assert 'Kaggle' in collectors.available_repos


def test_openml_availability():
    try:
        import openml
    except ImportError:
        assert 'OpenML' not in collectors.available_repos
    else:
        assert 'OpenML' in collectors.available_repos


def test_valid_request_output(api_collector):
    url = 'https://reqres.in/api/users/2'
    r, output = api_collector.get_request_output(url=url)

    assert r.status_code == 200
    assert 'data' in output


def test_all_empty(api_collector):
    data_dict = {
        'search': pd.DataFrame(),
        'metadata': pd.DataFrame()
    }
    assert api_collector._all_empty(data_dict)


def test_not_all_empty(api_collector):
    data_dict = {
        'search': pd.DataFrame({1: [1, 2, 3], 2: [1, 3, 5]}),
        'metadata': pd.DataFrame()
    }

    assert not api_collector._all_empty(data_dict)


def test_invalid_request_output(api_collector):
    url = 'https://example.com'
    with pytest.raises(RuntimeError, match=f'Query to {url}'):
        _ = api_collector.get_request_output(url=url)


def test_search_types(type_collector):
    assert 'dataset' not in type_collector.search_type_options
    assert 'users' in type_collector.search_type_options


def test_search_type_update(type_collector):
    with pytest.raises(ValueError):
        type_collector.search_types = ('users', 'customers')
