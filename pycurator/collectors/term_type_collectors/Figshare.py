from collections.abc import Collection
from typing import Optional, Union

import pandas as pd

from pycurator._typing import (
    SearchTerm,
    SearchType,
    TermTypeResultDict
)
from pycurator.collectors import (
    BaseCollector,
    BaseTermTypeCollector
)
from pycurator.utils.validating import validate_metadata_parameters


class FigshareCollector(BaseTermTypeCollector):
    """Figshare collector for search term and type queries.

    Parameters
    ----------
    search_terms : list-like, optional (default=None)
        Terms to search over. Can be (re)set via set_search_terms() or
        passed in directly to search functions.
    search_types : list-like, optional (default=None)
        Data types to search over. Can be (re)set via set_search_types()
        or passed in directly to search functions to override set
        parameter.
    credentials : str, optional (default=None)
        JSON filepath containing credentials in form
        {repository_name}: {key}.
    """

    def __init__(
        self,
        search_terms: Optional[Collection[SearchTerm]] = None,
        search_types: Optional[Collection[SearchType]] = None,
        credentials: Optional[str] = None
    ) -> None:
        super().__init__(
            repository_name='figshare',
            search_terms=search_terms,
            search_types=search_types,
            credentials=None
        )
        self.base_url = 'https://api.figshare.com/v2'
        self.merge_on = 'id'
        self.headers = dict()

        if credentials:
            self.credentials = self.load_credentials(
                credential_filepath=credentials
            )

    @staticmethod
    def accepts_user_credentials() -> bool:
        return True

    @classmethod
    @property
    def search_type_options(cls) -> tuple[SearchType, ...]:
        return ('articles', 'collections', 'projects')

    def load_credentials(self, credential_filepath: str) -> Union[str, None]:
        """Load the credentials given filepath or token.

        Parameters
        ----------
        credential_filepath : str
            JSON filepath containing credentials in the form
            {repository_name}: {key}.

        Returns
        -------
        credentials : str or None
        """

        credentials = super().load_credentials(
            credential_filepath=credential_filepath
        )
        self.headers['Authorization'] = f'token {credentials}'
        return credentials

    @BaseCollector._pb_indeterminate
    @BaseTermTypeCollector.validate_term_and_type
    def get_individual_search_output(
            self,
            search_term: SearchTerm,
            search_type: SearchType,
    ) -> Union[TermTypeResultDict, None]:
        """Queries the Figshare API for the given search term and type.

        Parameters
        ----------
        search_term : str
        search_type : {'articles', 'collections', 'projects'}

        Returns
        -------
        search_df : pandas.DataFrame

        Raises
        ------
        TypeError
            Incorrect search_term type.
        ValueError
            Invalid search_type provided.
        """

        start_page = 1
        page_size = 1000
        search_year = 1950
        search_date = f'{search_year}-01-01'
        search_url = f'{self.base_url}/{search_type}'

        search_df = pd.DataFrame()
        output_df = None

        search_params = {
            'search_for': search_term,
            'published_since': search_date,
            'order_direction': 'asc',
            'page': start_page,
            'page_size': page_size
        }

        response, output = self.get_request_output_and_update_query_ref(
            url=search_url,
            params=search_params,
            headers=self.headers,
            published_since=search_date,
            page=start_page
        )

        while response.status_code == 200:
            while response.status_code == 200 and output:
                output_df = pd.DataFrame(output)
                output_df['search_page'] = search_params['page']
                output_df['publish_query'] = search_params['published_since']

                search_df = pd.concat(
                    [search_df, output_df]
                ).reset_index(drop=True)

                search_params['page'] += 1
                response, output = \
                    self.get_request_output_and_update_query_ref(
                        url=search_url,
                        params=search_params,
                        headers=self.headers,
                        published_since=search_params['published_since'],
                        page=search_params['page']
                    )

            if (output_df is not None
                    and output_df.shape[0] < search_params['page_size']):
                return search_df

            # Get new date to search
            search_date = search_df['published_date'].values[-1].split('T')[0]
            search_params['published_since'] = search_date
            search_params['page'] = start_page

            # Conduct search
            response, output = self.get_request_output_and_update_query_ref(
                url=search_url,
                params=search_params,
                headers=self.headers,
                published_since=search_params['published_since'],
                page=search_params['page']
            )

        return search_df

    def get_query_metadata(
            self,
            object_paths: Union[str, Collection[str]],
    ) -> pd.DataFrame:
        """
        Retrieves metadata for the object_paths objects.

        Parameters
        ----------
        object_paths : str or list-like
            string or list of strings containing object paths.

        Returns
        -------
        metadata_df : pandas.DataFrame
            Metadata for the requested objects.
        """

        object_paths = validate_metadata_parameters(object_paths)

        metadata_df = pd.DataFrame()

        for object_path in self._pb_determinate(object_paths):
            _, json_data = self.get_request_output(
                url=object_path,
                headers=self.headers
            )

            metadata_df = pd.concat(
                [metadata_df, pd.DataFrame([json_data])]
            ).reset_index(drop=True)

        return metadata_df

    def get_all_metadata(
            self,
            search_dict: TermTypeResultDict
    ) -> TermTypeResultDict:
        """Retrieves metadata for records contained in input DataFrames.

        Parameters
        ----------
        search_dict : dict
            Dictionary of DataFrames from get_all_search_outputs.

        Returns
        -------
        metadata_dict : dict of tuple of str to dataframe
        """

        object_path_dict = dict()

        for query, df in search_dict.items():
            if df is not None:
                _, search_type = query
                object_ids = df.id.convert_dtypes().tolist()
                object_paths = [
                    f'{self.base_url}/{search_type}/{object_id}'
                    for object_id in object_ids
                ]

                object_path_dict[query] = object_paths

        metadata_dict = super().get_all_metadata(
            object_path_dict=object_path_dict
        )

        return metadata_dict
