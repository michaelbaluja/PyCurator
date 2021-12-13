import json
import os
from collections import OrderedDict

import pandas as pd
from flatten_json import flatten
from kaggle import KaggleApi
from kaggle.rest import ApiException
from tqdm import tqdm

from scrapers.base_scrapers import AbstractTermTypeScraper


class KaggleScraper(AbstractTermTypeScraper):
    """Scrapes Kaggle API for all data relating to the given search params.

    Parameters
    ----------
    search_terms : list-like, optional (default=None)
        Terms to search over. Can be (re)set via set_search_terms() or passed in
        directly to search functions.
    search_types : list-like, optional (default=None)
        Data types to search over. Can be (re)set via set_search_types() or 
        passed in directly to search functions to override set parameter.
    flatten_output : boolean, optional (default=None)
        Flag for specifying if nested output should be flattened. Can be passed
        in directly to functions to override set parameter.
    credentials : str, optional
        API token or pkl filepath containing credentials in dict.
        If filepath, data in file must be formatted as a dictionary of the form
        data_dict['{REPO_NAME}_TOKEN']: MY_KEY, or as a string containing the 
        key.

    Note: While the KaggleScraper has an option for inputting credentials, the
    functionality is deprecated, and only serves to allow base class 
    compatability. For validating Kaggle requests, read the official 
    documentation on authentication at https://www.kaggle.com/docs/api.
    """

    search_type_options = ('datasets', 'kernels')

    def __init__(self, search_terms=None, search_types=None,
                 flatten_output=None, credentials=None):

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

    def get_individual_search_output(self, search_term, search_type, **kwargs):
        """Calls the Kaggle API for the specified search term and type.

        Parameters
        ----------
        search_term : str
        search_type : str
        kwargs : dict, optional
            Can temporarily overwrite self flatten_output argument.

        Returns
        -------
        df : DataFrame
        """

        flatten_output = kwargs.get('flatten_output', self.flatten_output)

        # Validate input
        if not isinstance(search_term, str):
            raise ValueError('Search term must be a string.')
        if search_type not in KaggleScraper.search_type_options:
            raise ValueError('Can only search datasets, kernels.')

        # Use search type to get relevant API function
        list_queries = getattr(self.api, f'{search_type}_list')

        page_idx = 1
        search_df = pd.DataFrame()

        # Pulls a single page of results for the given search term
        self._print_progress(page_idx)
        output = list_queries(search=search_term, page=page_idx)

        # Search until we no longer recieve results
        while output:
            if search_type == 'kernels':
                output = [vars(result) for result in output]
            if flatten_output:
                output = [flatten(result) for result in output]

            output_df = pd.DataFrame(output)
            output_df['page'] = page_idx

            search_df = pd.concat([search_df, output_df]).reset_index(drop=True)

            # Increment page count for searching
            page_idx += 1

            # Pull next page of results
            self._print_progress(page_idx)
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
        else:
            return None

    def _retrieve_object_json(self, object_path, **kwargs):
        """Queries Kaggle for metadata json file & returns as a dict.

        Parameters
        ----------
        object_path : str
        data_path : str, optional (default='data/')
            Location to save metadata to
        kwargs : dict, optional
            Can temporarily overwrite self flatten_output argument.
        """

        flatten_output = kwargs.get('flatten_output', self.flatten_output)
        data_path = kwargs.get('data_path', f'data{os.sep}')

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
        kwargs : dict, optional

        Returns
        -------
        metadata_df : DataFrame
        """

        flatten_output = kwargs.get('flatten_output', self.flatten_output)

        # Ensure object paths are of the proper form
        object_paths = self.validate_metadata_parameters(object_paths)

        # Create hollow output DataFrame
        metadata_df = pd.DataFrame()

        # Pulls meatadata information for each object
        for object_path in tqdm(object_paths):
            # Download and load the metadata
            json_data = self._retrieve_object_json(
                object_path, 
                flatten_output=flatten_output
            )
            
            # Store the metadata info in cumulative df
            metadata_df = metadata_df.append(json_data, ignore_index=True)

        # Modify dtypes for uniformity
        metadata_df = metadata_df.convert_dtypes()

        return metadata_df

    def get_all_metadata(self, search_dict, **kwargs):
        """Retrieves all related metadata for the provided DataFrames.

        Parameters
        ----------
        search_dict : Iterable of DataFrames
            Output from get_all_search_outputs function.
        kwargs : dict, optional
            Can temporarily overwrite self flatten_output argument.
        
        Returns
        -------
        metadata_dict : OrderedDict of DataFrames
            Stores the results of each call to get_query_metadata in the form 
            metadata_dict[(search_term, search_type)] = df.
        """

        flatten_output = kwargs.get('flatten_output', self.flatten_output)
        object_path_dict = OrderedDict()

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
