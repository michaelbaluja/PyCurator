import pytest

from pycurator import collectors


@pytest.fixture
def metadata_params():
    return ['100', '501', '200', '300']


@pytest.fixture
def sample_args(metadata_params):
    return (metadata_params, 'users')


@pytest.fixture
def sample_kwargs(metadata_params):
    return {
        'object_paths': metadata_params,
        'search_type': 'users'
    }


@pytest.fixture
def api_collector():
    return collectors.BaseAPICollector('reqres')


@pytest.fixture
def type_collector():
    class TestTypeCollector(collectors.BaseTypeCollector):
        def __init__(self):
            super().__init__(repository_name='base_term_type')
            self.base_url = 'https://reqres.in/api'

        def get_query_metadata(self, object_paths, search_type):
            pass

        def get_all_metadata(self, **kwargs):
            pass

        def accepts_user_credentials(self):
            return False

        @classmethod
        @property
        def search_type_options(cls):
            return ('users',)

        def get_individual_search_output(
                self,
                search_type
        ):
            search_url = f'{self.base_url}/{search_type}'
            search_params = {'page': 1}

            return self.get_request_output(
                url=search_url,
                params=search_params
            )

    return TestTypeCollector()
