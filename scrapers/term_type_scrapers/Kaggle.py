import json
import os

import pandas as pd
from flatten_json import flatten
from kaggle import KaggleApi
from kaggle.rest import ApiException

from scrapers.base_scrapers import AbstractScraper, AbstractTermTypeScraper


class KaggleScraper(AbstractTermTypeScraper):
    """Scrapes Kaggle API for all data relating to the given search params.

    Parameters
    ----------
    search_terms : list-like, optional (default=None)
        Terms to search over. Can be (re)set via set_search_terms() or passed
        in directly to search functions.
    search_types : list-like, optional (default=None)
        Data types to search over. Can be (re)set via set_search_types() or
        passed in directly to search functions to override set parameter.
    flatten_output : bool, optional (default=None)
        Flag for specifying if nested output should be flattened. Can be passed
        in directly to functions to override set parameter.
    credentials : str, optional (default=None)
        JSON filepath containing credentials in form {repository_name}: 'key'.

    Notes
    -----
    For validating Kaggle requests, read the official documentation on
    authentication at https://www.kaggle.com/docs/api.
    """

    def __init__(
        self,
        search_terms=None,
        search_types=None,
        flatten_output=None
    ):

        super().__init__(
            repository_name='kaggle',
            search_terms=search_terms,
            search_types=search_types,
            flatten_output=flatten_output
        )

        self.api = KaggleApi()
        self.api.authenticate()
        self.merge_on = 'id'

    @staticmethod
    def accept_user_credentials():
        return False

    @classmethod
    def get_search_type_options(cls):
        return ('datasets', 'kernels')

    @AbstractScraper._pb_indeterminate
    def get_individual_search_output(self, search_term, search_type, **kwargs):
        """Calls the Kaggle API for the specified search term and type.

        Parameters
        ----------
        search_term : str
        search_type : str
        **kwargs : dict, optional
            Can temporarily overwrite self flatten_output argument.

        Returns
        -------
        search_df : pandas.DataFrame
        """

        flatten_output = kwargs.get('flatten_output', self.flatten_output)
        search_type_options = self.get_search_type_options()

        # Validate input
        if not isinstance(search_term, str):
            raise ValueError('Search term must be a string.')
        if search_type not in search_type_options:
            raise ValueError(f'Can only search {search_type_options}.')

        # Use search type to get relevant API function
        list_queries = getattr(self.api, f'{search_type}_list')

        page_idx = 1
        search_df = pd.DataFrame()

        # Pulls a single page of results for the given search term
        self._update_query_ref(page='page_idx')
        output = list_queries(search=search_term, page=page_idx)

        # Search until we no longer recieve results
        while output:
            # If user has requested termination, handle cleanup
            if not self.continue_running:
                self.terminate()

            if search_type == 'kernels':
                output = [vars(result) for result in output]
            if flatten_output:
                output = [flatten(result) for result in output]

            output_df = pd.DataFrame(output)
            output_df['page'] = page_idx

            search_df = pd.concat(
                [search_df, output_df]
            ).reset_index(drop=True)

            # Increment page count for searching
            page_idx += 1

            # Pull next page of results
            self._update_query_ref(page=page_idx)
            output = list_queries(search=search_term, page=page_idx)

        # Only modify if the DataFrame contains data
        if not search_df.empty:
            # Modify columns for metadata merge
            search_df = search_df.rename(
                columns={'id': 'datasetId', 'ref': 'id'}
            )

            if search_type == 'datasets':
                search_df = search_df.drop(columns={'viewCount', 'voteCount'})

            search_df = search_df.convert_dtypes()

        return search_df

    def _retrieve_object_json(self, object_path, **kwargs):
        """Queries Kaggle for metadata json file & returns as a dict.

        Parameters
        ----------
        object_path : str
        data_path : str, optional (default='data/')
            Location to save metadata to
        **kwargs : dict, optional
            Can temporarily overwrite self flatten_output argument.

        Returns
        -------
        json_data : dict or None

        Raises
        ------
        kaggle.rest.ApiException
            A query was made that was unable to be fulfilled.

        See Also
        --------
        kaggle
        """

        flatten_output = kwargs.get('flatten_output', self.flatten_output)
        data_path = kwargs.get('data_path', f'data{os.sep}')

        # If user has requested termination, handle cleanup instead of querying
        # additional results
        if not self.continue_running:
            self.terminate()

        # Download the metadata
        try:
            self.api.dataset_metadata(object_path, path=data_path)
        except (TypeError, ApiException) as e:
            if (isinstance(e, ApiException) and e.status != 404
                    and 'bigquery' not in e.headers['Turbolinks-Location']):
                raise e
            else:
                return None
        else:
            # Access the metadata and load it in as a a dictionary
            metadata_file_path = f'{data_path}dataset-metadata.json'
            with open(metadata_file_path) as f:
                json_data = json.load(f)

            # Delete metadata file (no longer needed)
            os.remove(metadata_file_path)

            if flatten_output:
                json_data = flatten(json_data)

            return json_data

    def get_query_metadata(self, object_paths, **kwargs):
        """Retrieves the metadata for the objects referenced in object_paths.

        Parameters
        ----------
        object_paths : str/list-like
        **kwargs : dict, optional

        Returns
        -------
        metadata_df : pandas.DataFrame
        """

        flatten_output = kwargs.get('flatten_output', self.flatten_output)
        object_paths = self.validate_metadata_parameters(object_paths)

        metadata_df = pd.DataFrame()

        for object_path in self._pb_determinate(object_paths):
            json_data = self._retrieve_object_json(
                object_path,
                flatten_output=flatten_output
            )

            metadata_df = metadata_df.append(json_data, ignore_index=True)

        metadata_df = metadata_df.convert_dtypes()

        return metadata_df

    def get_all_metadata(self, search_dict, **kwargs):
        """Retrieves all related metadata for the provided DataFrames.

        Parameters
        ----------
        search_dict : Iterable of pandas.DataFrames
            Output from get_all_search_outputs function.
        **kwargs : dict, optional
            Can temporarily overwrite self flatten_output argument.

        Returns
        -------
        metadata_dict : dict of DataFrames
            Stores the results of each call to get_query_metadata in the form
            metadata_dict[(search_term, search_type)] = df.
        """

        flatten_output = kwargs.get('flatten_output', self.flatten_output)
        object_path_dict = dict()

        for query, df in search_dict.items():
            # Only want to get metadata for non-empty dataset DataFrames
            if 'kernels' not in query and df is not None:
                # Extract object paths
                object_paths = df.id.values
                object_path_dict[query] = object_paths

        metadata_dict = super().get_all_metadata(
            object_path_dict,
            flatten_output=flatten_output
        )

        return metadata_dict
