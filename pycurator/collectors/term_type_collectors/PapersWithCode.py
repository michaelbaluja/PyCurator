from collections.abc import Collection
from typing import Any, Optional, Union

import pandas as pd

from pycurator._typing import (
    SearchTerm,
    SearchType,
    SearchQuery,
    TermResultDict,
    TermTypeResultDict,
    TypeResultDict
)
from pycurator.collectors import (
    BaseCollector,
    BaseTermTypeCollector
)
from pycurator.utils.validating import validate_metadata_parameters


class PapersWithCodeCollector(BaseTermTypeCollector):
    """PapersWithCode collector for search term and type queries.

    Parameters
    ----------
    search_terms : list-like, optional (default=None)
        Terms to search over. Can be (re)set via set_search_terms()
        or passed in directly to search functions to override set
        parameter.
    search_types : list-like, optional
        Types to search over. Can be (re)set via set_search_types()
        or passed in directly to search functions.
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
            repository_name='paperswithcode',
            search_terms=search_terms,
            search_types=search_types,
            credentials=credentials
        )
        self.base_url = 'https://paperswithcode.com/api/v1'

    @staticmethod
    def accepts_user_credentials() -> bool:
        return True

    @classmethod
    @property
    def search_type_options(cls) -> tuple[SearchType, ...]:
        return ('conferences', 'datasets', 'evaluations', 'papers', 'tasks')

    @BaseCollector._pb_indeterminate
    def _conduct_search_over_pages(
        self,
        search_url: str,
        search_params: Optional[Any] = None,
        print_progress: bool = False
    ) -> Union[pd.DataFrame, None]:
        """Query paginated results from the Papers With Code API.

        Parameters
        ----------
        search_url : str
        search_params : dict
            Contains parameters to pass to requests.get({params}). Most
            common include search term 'q', and page index 'page'. For
            full details, see the Notes.
        print_progress : bool, optional (default=False)
            If True, updates on query page progress is sent to object
            queue to be displayed in UI window.

        Returns
        -------
        search_df : pandas.DataFrame or None

        Notes
        -----
        For querying base objects, the following parameters are most common:
        q : str, optional
            Term to query for.
        page : int, optional
            Page number to request.
        items_per_page : int, optional
            Number of results to return.
        ordering : str, optional
            Which field to order results by.

        For more detailed parameters, visit
        https://paperswithcode.com/api/v1/docs/
        """

        search_df = pd.DataFrame()

        if print_progress:
            self._update_query_ref(page=search_params['page'])
        _, output = self.get_request_output(
            url=search_url,
            params=search_params
        )

        while output.get('results'):
            output = output['results']

            output_df = pd.DataFrame(output)
            output_df['page'] = search_params['page']

            search_df = pd.concat(
                [search_df, output_df]
            ).reset_index(drop=True)

            search_params['page'] += 1

            if print_progress:
                self._update_query_ref(page=search_params['page'])

            response, output = self.get_request_output(
                url=search_url,
                params=search_params
            )

            # Ensure we've received results if they exist
            # 200: OK, 404: page not found (no more results)
            while response.status_code not in [200, 404]:
                self.status_queue.put(
                    f'Search error "{response.status_code}" on '
                    f'page {search_params["page"]}'
                )
                search_params['page'] += 1

                if print_progress:
                    self._update_query_ref(page=search_params['page'])

                response, output = self.get_request_output(
                    url=search_url,
                    params=search_params
                )

        if not search_df.empty:
            return search_df
        else:
            return None

    @BaseTermTypeCollector.validate_term_and_type
    def get_individual_search_output(
            self,
            search_term: SearchTerm,
            search_type: SearchType
    ) -> pd.DataFrame:
        """Queries Papers With Code API for the specified search term and type.

        Parameters
        ----------
        search_term : str
        search_type : {
            'conferences', 'datasets', 'evaluations', 'papers', 'tasks'
        }

        Returns
        -------
        pandas.DataFrame

        Raises
        ------
        TypeError
            Incorrect search_term type.
        ValueError
            Invalid search_type provided.
        """

        search_url = f'{self.base_url}/{search_type}'

        search_params = {
            'q': search_term,
            'page': 1,
            'items_per_page': 500
        }

        return self._conduct_search_over_pages(
            search_url=search_url,
            search_params=search_params,
            print_progress=True
        )

    @BaseTermTypeCollector.validate_search_type
    def _get_metadata_types(
            self,
            search_type: SearchType
    ) -> Collection[SearchType]:

        if search_type == 'conferences':
            return ['proceedings']
        elif search_type == 'datasets':
            return ['evaluations']
        elif search_type == 'evaluations':
            return ['metrics', 'results']
        elif search_type == 'papers':
            return ['datasets', 'methods', 'repositories', 'results', 'tasks']
        elif search_type == 'tasks':
            return ['children', 'evaluations', 'papers', 'parents']

    def get_query_metadata(
            self,
            object_paths: Collection[str],
            **kwargs: Any
    ) -> TypeResultDict:
        """Retrieves metadata for the object_paths objects.

        Parameters
        ----------
        object_paths : str or collection of str
        **kwargs : dict, optional
            Must include search_type : {
                'conferences', 'datasets', 'evaluations', 'papers', 'tasks'
            }
            Search_type in kwargs to match signature of base method.

        Returns
        -------
        metadata_dict : dict
            Results are stored in the format
            metadata_dict[metadata_type] = DataFrame
        """

        try:
            search_type = kwargs['search_type']
        except KeyError:
            raise AttributeError(
                'search_type must be passed to get_query_metadata.'
            )
        object_paths = validate_metadata_parameters(object_paths)

        metadata_types = self._get_metadata_types(search_type)
        metadata_dict = dict()

        for metadata_type in metadata_types:
            search_df = pd.DataFrame()
            self.status_queue.put(f'Querying {metadata_type}.')

            for object_path in self._pb_determinate(object_paths):
                search_url = f'{self.base_url}/{search_type}/{object_path}/' \
                             f'{metadata_type}'
                search_params = {'page': 1}

                # Conduct the search and add supplementary info to DataFrame
                object_df = self._conduct_search_over_pages(
                    search_url=search_url,
                    search_params=search_params
                )

                if object_df is not None:
                    object_df['id'] = object_path
                    object_df['page'] = search_params['page']

                search_df = pd.concat(
                    [search_df, object_df]
                ).reset_index(drop=True)

            if not search_df.empty:
                metadata_dict[metadata_type] = search_df

        return metadata_dict

    def get_all_metadata(
            self,
            search_dict: TermTypeResultDict
    ) -> dict[SearchQuery, TermResultDict]:
        """Retrieves metadata for records contained in input DataFrames.

        Parameters
        ----------
        search_dict : dict
            Dictionary of DataFrames from get_all_search_outputs.

        Returns
        -------
        metadata_dict : dict
        """

        metadata_dict = dict()

        for query, df in search_dict.items():
            if df is not None:
                search_term, search_type = query
                self.status_queue.put(
                    f'Querying {search_term} {search_type} metadata.'
                )

                object_paths = df.id.values

                metadata_dict[query] = self.get_query_metadata(
                    object_paths=object_paths,
                    search_type=search_type
                )

                self.status_queue.put(
                    f'{search_term} {search_type} metadata query complete.'
                )

        return metadata_dict

    def merge_search_and_metadata_dicts(
        self,
        search_dict,
        metadata_dict,
        on=None,
        left_on=None,
        right_on=None,
        **kwargs
    ):
        """Merges together search and metadata DataFrames by 'on' value.

        Parameters
        ----------
        search_dict : dict
            Dictionary of search output results.
        metadata_dict : dict
            Dictionary of metadata results.
        on : str or list-like, optional (default=None)
            Column name(s) to merge the two dicts on.
        left_on : str or list-like, optional (default=None)
            Column name(s) to merge the left dict on.
        right_on : str or list-like, optional (default=None)
            Column name(s) to merge the right dict on.
        **kwargs : dict, optional
            Allow users to add save value.

        Returns
        -------
        merged_dict : dict

        See Also
        --------
        pandas.merge
        """

        merged_dict = dict()

        for query_key, type_df_dict in metadata_dict.items():
            search_term, search_type = query_key
            search_df = search_dict[query_key]

            for metadata_type, metadata_df in type_df_dict.items():
                _search_type = f'{search_type}_{metadata_type}'
                df_all = pd.merge(
                    left=search_df,
                    right=metadata_df,
                    on='id',
                    left_on=left_on,
                    right_on=right_on,
                    how='outer',
                    suffixes=('_search', '_metadata')
                )

                merged_dict[(search_term, _search_type)] = df_all

        return merged_dict
