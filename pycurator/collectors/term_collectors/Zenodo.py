import warnings
from collections.abc import Collection
from typing import Any, Optional, Iterable, NoReturn

import pandas as pd

from pycurator._typing import SearchTerm
from pycurator.collectors import BaseCollector, BaseTermCollector


class ZenodoCollector(BaseTermCollector):
    """Zenodo collector for search term queries.

    Parameters
    ----------
    search_terms : list-like, optional
        Terms to search over. Can be (re)set via set_search_terms()
        or passed in directly to search functions.
    credentials : str, optional (default=None)
        JSON filepath containing credentials in form
        {repository_name}: {key}.
    """

    def __init__(
        self,
        search_terms: Optional[Collection[SearchTerm]] = None,
        credentials: Optional[bool] = None
    ) -> None:
        super().__init__(
            repository_name='zenodo',
            search_terms=search_terms,
            credentials=credentials
        )
        self.base_url = 'https://zenodo.org/api/records'

    @staticmethod
    def accepts_user_credentials() -> bool:
        return True

    @BaseTermCollector.validate_search_term
    @BaseCollector._pb_indeterminate
    def get_individual_search_output(
            self,
            search_term: SearchTerm
    ) -> pd.DataFrame:
        """Returns information about all records from Zenodo.

        Parameters
        ----------
        search_term : str

        Returns
        -------
        search_df : pandas.DataFrame

        Raises
        ------
        TypeError
            Incorrect search_term type.

        Warns
        -----
        RuntimeWarning
            Unsuccessful query response from the API.
        """

        search_year = 2022
        search_df = pd.DataFrame()
        start_date = f'{search_year}-01-01'
        end_date = f'{search_year}-12-31'

        search_params = {
            'q': f'{search_term} AND created:[{start_date} TO {end_date}]',
            'page': 1,
            'size': 1000
        }

        response, output = self.get_request_output_and_update_query_ref(
            url=self.base_url,
            params=search_params,
            search_term=search_term,
            year=search_year,
            page=search_params['page']
        )

        # Handle any potential errors
        if response.status_code != 200:
            warnings.warn(
                f'{response.status_code}: Returning without results.',
                RuntimeWarning
            )
            self.continue_running = False
            self.terminate()

        while output.get('hits').get('total'):
            while (
                response.status_code == 200 and
                output.get('hits').get('hits')
            ):
                output = output['hits']['hits']

                output_df = pd.DataFrame(output)
                output_df['page'] = search_params['page']

                search_df = pd.concat(
                    [search_df, output_df]
                ).reset_index(drop=True)

                search_params['page'] += 1
                response, output = \
                    self.get_request_output_and_update_query_ref(
                        url=self.base_url,
                        params=search_params,
                        search_term=search_term,
                        year=search_year,
                        page=search_params['page']
                    )

            search_year -= 1
            start_date = f'{search_year}-01-01'
            end_date = f'{search_year}-12-31'

            search_params['q'] = \
                f'{search_term} AND created:[{start_date} TO {end_date}]'
            search_params['page'] = 1

            response, output = self.get_request_output_and_update_query_ref(
                url=self.base_url,
                params=search_params,
                search_term=search_term,
                year=search_year,
                page=search_params['page']
            )

        self.num_queries = False

        return search_df

    def get_query_metadata(self, object_paths: Iterable[Any]) -> NoReturn:
        raise NotImplementedError(
            'Zenodo does not provide object metadata.'
        )
