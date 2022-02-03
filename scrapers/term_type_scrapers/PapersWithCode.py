from collections import OrderedDict

import pandas as pd
import requests
from flatten_json import flatten
from tqdm import tqdm

from scrapers.base_scrapers import AbstractScraper, AbstractTermTypeScraper


class PapersWithCodeScraper(AbstractTermTypeScraper):
    """Scrapes PapersWithCode API for all data for the given search params.

    Parameters
    ----------
    search_terms : list-like, optional (default=None)
        Terms to search over. Can be (re)set via set_search_terms() or passed in
        directly to search functions to override set parameter.
    search_types : list-like, optional
        Types to search over. Can be (re)set via set_search_types() or passed in
        directly to search functions.
    flatten_output : boolean, optional (default=None)
        Flag for specifying if nested output should be flattened. Can be passed
        in directly to functions to override set parameter.
    credentials : str, optional (default=None)
        JSON filepath containing credentials in form {repository_name}: 'key'.
    """

    search_type_options = (
        'conferences', 
        'datasets', 
        'evaluations', 
        'papers', 
        'tasks'
    )

    def __init__(
        self,
        search_terms=None,
        search_types=None,
        flatten_output=None,
        credentials=None
    ):
        super().__init__(
            repository_name='paperswithcode',
            search_terms=search_terms,
            search_types=search_types,
            flatten_output=flatten_output,
            credentials=credentials
        )
        self.base_url = 'https://paperswithcode.com/api/v1'

    @staticmethod
    def accept_user_credentials():
        return True

    @AbstractScraper._pb_indeterminate
    def _conduct_search_over_pages(
        self,
        search_url,
        search_params,
        flatten_output,
        print_progress=False
    ):
        search_df = pd.DataFrame()

        # Conduct a search, extract json results
        if print_progress:
            self._update_query_ref(page=search_params['page'])
        response, output = self.get_request_output(
            url=search_url, 
            params=search_params
        )

        # Search over all valid pages
        while output.get('results'):
            # Extract relevant results
            output = output['results']

            # Flatten nested json
            if flatten_output:
                output = [flatten(result) for result in output]

            # Add results to cumulative DataFrame
            output_df = pd.DataFrame(output)
            output_df['page'] = search_params['page']

            search_df = pd.concat([search_df, output_df]).reset_index(drop=True)

            # Increment page for search
            search_params['page'] += 1

            # Conduct a search
            if print_progress:
                self._update_query_ref(page=search_params['page'])

            response, output = self.get_request_output(
                url=search_url,
                params=search_params
            )

            # Ensure we've received results if they exist
            # 200: OK, 404: page not found (no more results)
            while response.status_code not in [200, 404]:
                self.queue.put(
                    f'Search error "{response.status_code}" on '
                    f'page {search_params["page"]}'
                )
                search_params['page'] += 1

                # Conduct a search
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

    def get_individual_search_output(self, search_term, search_type, **kwargs):
        """Returns information about all queried information types on PWC.

        Parameters
        ----------
        search_term : str
        search_type : str
            Must be one of:
            ('conferences', 'datasets', 'evaluations', 'papers', 'tasks')
        kwargs : dict, optional
            Can temporarily overwrite self flatten_output argument.

        Returns
        -------
        pandas.DataFrame
        """

        if not isinstance(search_term, str):
            raise ValueError('Search term must be a string.')
        if search_type not in PapersWithCodeScraper.search_type_options:
            raise ValueError(
                'Search type must be one of '
                f'{PapersWithCodeScraper.search_type_options}'
            )

        flatten_output = kwargs.get('flatten_output', self.flatten_output)
        search_url = f'{self.base_url}/{search_type}'

        search_params = {
            'q': search_term,
            'page': 1,
            'items_per_page': 500  # Max size
        }

        return self._conduct_search_over_pages(
            search_url=search_url,
            search_params=search_params,
            flatten_output=flatten_output,
            print_progress=True
        )

    def _get_metadata_types(self, search_type):
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
        else:
            raise ValueError(
                f'Incorrect search type "{search_type}" passed in')

    def get_query_metadata(self, object_paths, search_type, **kwargs):
        """Retrieves the metadata for the papers listed in object_paths

        Parameters
        ----------
        object_paths : str/list-like
        search_type : str
        kwargs : dict, optional
            Can temporarily overwrite self flatten_output argument

        Returns
        -------
        metadata_dict : dict
            Results are stored in the format 
            metadata_dict[metadata_type] = DataFrame
        """

        flatten_output = kwargs.get('flatten_output', self.flatten_output)

        # Ensure object paths are of the proper form
        object_paths = self.validate_metadata_parameters(object_paths)

        metadata_types = self._get_metadata_types(search_type)
        metadata_dict = OrderedDict()

        for metadata_type in metadata_types:
            search_df = pd.DataFrame()
            self.queue.put(f'Querying {metadata_type}.')

            for object_path in self._pb_determinate(object_paths):
                search_url = f'{self.base_url}/{search_type}/{object_path}/' \
                             f'{metadata_type}'
                search_params = {'page': 1}

                # Conduct the search and add supplementary info to DataFrame
                object_df = self._conduct_search_over_pages(
                    search_url,
                    search_params,
                    flatten_output
                )
                
                if object_df is not None:
                    object_df['id'] = object_path
                    object_df['page'] = search_params['page']

                # Merge with the cumulative search DataFrame
                search_df = pd.concat(
                    [search_df, object_df]
                ).reset_index(drop=True)

            if not search_df.empty:
                metadata_dict[metadata_type] = search_df

        return metadata_dict

    def get_all_metadata(self, search_dict, **kwargs):
        """Retrieves all metadata related to the provided DataFrames.

        Parameters
        ----------
        search_dict : dict
            Dictionary of DataFrames from get_all_search_outputs.
        kwargs : dict, optional
            Can temporarily overwrite self flatten_output argument.
        
        Returns
        -------
        metadata_dict : OrderedDict
            OrderedDict of dicts with metadata for each query.
            Order matches the order of search_dict.
        """

        metadata_dict = OrderedDict()

        for query, df in search_dict.items():
            if df is not None:
                search_term, search_type = query
                self.queue.put(
                    f'Querying {search_term} {search_type} metadata.'
                )
                
                object_paths = df.id.values

                metadata_dict[query] = self.get_query_metadata(
                    object_paths=object_paths,
                    search_type=search_type,
                    **kwargs
                )

                self.queue.put(
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

    ):
        """Merges together search and metadata DataFrames by the given 'on' key.

        Parameters
        ----------
        search_dict : dict
            Dictionary of search output results.
        metadata_dict : dict
            Dictionary of metadata results.
        on : str/list-like, optional (default=None)
            Column name(s) to merge the two dicts on.
        left_on : str/list-like, optional (default=None)
            Column name(s) to merge the left dict on.
        right_on : str/list-like, optional (default=None)
            Column name(s) to merge the right dict on.
        kwargs : dict, optional
            Allow users to add save value.

        Returns
        -------
        df_dict : OrderedDict
            OrderedDict containing all of the merged search/metadata dicts.
        """

        df_dict = OrderedDict()

        for query_key, type_df_dict in metadata_dict.items():
            search_term, search_type = query_key
            search_df = search_dict[query_key]

            for metadata_type, metadata_df in type_df_dict.items():
                _search_type = f'{search_type}_{metadata_type}'
                df_all = pd.merge(
                    search_df,
                    metadata_df,
                    on='id',
                    left_on=left_on,
                    right_on=right_on,
                    how='outer',
                    suffixes=('_search', '_metadata')
                )

                df_dict[(search_term, _search_type)] = df_all
        
        return df_dict
