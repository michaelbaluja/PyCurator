import os
import pickle
from abc import ABC, abstractmethod
from collections import OrderedDict

import pandas as pd
import requests
from flatten_json import flatten
from tqdm import tqdm

from .base_scrapers import AbstractAPIScraper


class AbstractTermScraper(AbstractAPIScraper):
    """Base Class for scraping repository API's based on search term.

    Parameters
    ----------
    repository_name : str
        Name of the repository being scraped. Used for loading credentials and
        saving output results.
    search_terms : list-like, optional (default=None)
        Terms to search over. Can be (re)set via set_search_terms() or passed in
        directly to search functions to override set parameter.
    flatten_output : boolean, optional (default=False)
        Flag for specifying if nested output should be flattened. Can be passed
        in directly to functions to override set parameter.
    credentials : str, optional (default=None)
        API token or pkl filepath containing credentials in dict.
        If filepath, data in file must be formatted as a dictionary of the form
        data_dict['{REPO_NAME}_TOKEN']: MY_KEY, or as a string containing the 
        key.
    """

    def __init__(
        self, 
        repository_name, 
        search_terms=None, 
        flatten_output=False, 
        credentials=None
    ):
        super().__init__(repository_name, flatten_output, credentials)

        if search_terms:
            self.set_search_terms(search_terms)

    def set_search_terms(self, search_terms):
        self.search_terms = search_terms

    def run(self, **kwargs):
        """Queries all data from the implemented API.

        In the following order, this function calls:
        - get_all_search_outputs
        - get_all_metadata (if applicable)
        - merge_search_and_metadata_dicts (if applicable)

        Parameters
        ----------
        kwargs : dict, optional
            Can temporarily overwrite self attributes.

        Returns
        -------
        merged_dict/search_dict : dict
            Returns merged_dict if metadata is available. This is the output of
                the merge_search_and_metadata_dicts function.
            Returns search_dict if metadata is not available. This is the 
                output of get_all_search_outputs.
        """

        # Get search_output
        search_dict = self.get_all_search_outputs(**kwargs)

        # Set merge parameters
        merge_on = vars(self).get('merge_on')
        merge_right_on = vars(self).get('merge_right_on')
        merge_left_on = vars(self).get('merge_left_on')

        # Set save parameters
        save_dataframe = kwargs.get('save_dataframe')

        # Try to get metadata (if available)
        try:
            metadata_dict = self.get_all_metadata(
                    search_dict=search_dict, 
                    **kwargs
                )
            merged_dict = self.merge_search_and_metadata_dicts(
                search_dict=search_dict,
                metadata_dict=metadata_dict,
                on=merge_on,
                left_on=merge_left_on,
                right_on=merge_right_on
            )
            final_dict = merged_dict
        except (AttributeError, TypeError) as e:
            # Attribute Error: Tries to call a function that does not exist
            # TypeError: Tries to call function with incorrect arguments
            print(e)
            final_dict = search_dict

        if save_dataframe:        
            try:
                save_dir = kwargs['save_dir']
            except KeyError:
                raise ValueError('Must pass save directory to run function.')
            self.save_dataframes(final_dict, save_dir)

    def get_all_search_outputs(self, **kwargs):
        """Queries the API for each search term.

        Parameters
        ----------
        **kwargs : dict, optional
            Can temporarily overwrite self search_terms and flatten_output 
            arguments.

        Returns
        -------
        search_dict : OrderedDict of DataFrames
            Stores the results of each call to get_individual_search_output in
            the form search_dict[{search_term}] = df.
        """

        # Set method variables if different than default
        search_terms = kwargs.get('search_terms', self.search_terms)
        flatten_output = kwargs.get('flatten_output', self.flatten_output)

        search_dict = OrderedDict()

        for search_term in search_terms:
            print(f'Searching {search_term}.')
            search_dict[search_term] = self.get_individual_search_output(
                search_term, flatten_output=flatten_output)
            print('Search completed.', flush=True)

        return search_dict

    @abstractmethod
    def get_individual_search_output(self, search_term, **kwargs):
        print('abstract metadata')

    def get_all_metadata(self, object_path_dict, **kwargs):
        """Retrieves all of the metadata that relates to the provided DataFrames.
        
        Parameters
        ----------
        object_path_dict : dict
            Dict of the form {query: object_paths} for list of object paths.
        kwargs : dict, optional 
            Can temporarily overwrite self flatten_output argument.
        
        Returns
        -------
        metadata_dict : OrderedDict
            OrderedDict of DataFrames with metadata for each query.
            Order matches the order of search_dict.
        """
        
        flatten_output = kwargs.get('flatten_output', self.flatten_output)

        metadata_dict = OrderedDict()

        for query, object_paths in object_path_dict.items():
            print(f'Querying {query} metadata.')
            metadata_dict[query] = self.get_query_metadata(
                object_paths, flatten_output=flatten_output)
        
        return metadata_dict

    def get_query_metadata(self, object_paths, **kwargs):
        # We raise an error instead of requiring implementation via
        # @abstractmethod since not all derived classes will require it
        raise NotImplementedError


class DryadScraper(AbstractTermScraper):
    """Scrapes the Dryad API for all data relating to the given search terms.

    Parameters
    ----------
    search_terms : list-like, optional
        Terms to search over. Can be (re)set via set_search_terms() or passed in
        directly to search functions.
    flatten_output : boolean, optional (default=None)
        Flag for specifying if nested output should be flattened. Can be passed
        in directly to functions to override set parameter.
    credentials : str, optional
        API token or pkl filepath containing credentials in dict.
        If filepath, data in file must be formatted as a dictionary of the form
        data_dict['{REPO_NAME}_TOKEN']: MY_KEY, or as a string containing the 
        key.
    """

    def __init__(self, search_terms=None, flatten_output=None, credentials=None):
        super().__init__('dryad', search_terms, flatten_output, credentials)
        self.base_url = 'https://datadryad.org/api/v2'

        self.merge_on = 'id'

    @staticmethod
    def accept_user_credentials():
        return False

    def _conduct_search_over_pages(
        self, 
        search_url, 
        search_params, 
        flatten_output, 
        print_progress=False,
        delim=None
    ):
        search_df = pd.DataFrame()

        # Perform initial search and convert results to json
        if print_progress:
            self._print_progress(search_params['page'])
        _, output = self.get_request_output(search_url, params=search_params)

        # Queries next page as long as current page isn't empty
        while output.get('count'):
            # Extract relevant output data
            output = output['_embedded']

            if delim:
                output = output[delim]

            # Flatten output if necessary
            if flatten_output:
                output = [flatten(result) for result in output]

            # Convert output to df, add to cumulative
            output_df = pd.DataFrame(output)
            output_df['page'] = search_params['page']

            search_df = pd.concat([search_df, output_df]
                                  ).reset_index(drop=True)

            # Increment page to search over
            search_params['page'] += 1

            # Perform next search and convert results to json
            if print_progress:
                self._print_progress(search_params['page'])
            _, output = self.get_request_output(search_url, params=search_params)

        return search_df

    def get_individual_search_output(self, search_term, **kwargs):
        """Returns information about all datasets from Data Dryad.

        Parameters
        ----------
        search_term : str
        kwargs : dict, optional
            Can temporarily overwrite self flatten_output argument.

        Returns
        -------
        pandas.DataFrame
        """

        flatten_output = kwargs.get('flatten_output', self.flatten_output)

        # Set search params
        search_url = f'{self.base_url}/search'

        search_params = {'q': search_term, 'page': 1, 'per_page': 100}

        return self._conduct_search_over_pages(
            search_url=search_url, 
            search_params=search_params, 
            flatten_output=flatten_output, 
            print_progress=True,
            delim='stash:datasets'
        )

    def get_query_metadata(self, object_paths, **kwargs):
        """Retrieves the metadata for the file/files listed in object_paths.

        Parameters
        ----------
        object_paths : str/list-like
        kwargs : dict, optional
            Can temporarily overwrite self flatten_output argument.

        Returns
        -------
        metadata_df : DataFrame
        """

        flatten_output = kwargs.get('flatten_output', self.flatten_output)

        # Ensure object paths are of the proper form
        object_paths = self.validate_metadata_parameters(object_paths)

        # Set search variables
        start_page = 1
        metadata_df = pd.DataFrame()

        # Query the metadata for each object
        for object_path in tqdm(object_paths):
            search_url = f'{self.base_url}/versions/{object_path}/files'
            search_params = {'page': start_page}

            # Conduct search
            object_df = self._conduct_search_over_pages(
                search_url, 
                search_params, 
                flatten_output, 
                delim='stash:files'
            )

            # Add relevant data to DataFrame and merge
            object_df['id'] = object_path
            object_df['page'] = search_params['page']
            metadata_df = pd.concat(
                [metadata_df, object_df]).reset_index(drop=True)
        return metadata_df

    def get_all_metadata(self, search_dict, **kwargs):
        """Retrieves all metadata that relates to the provided DataFrames.
        
        Parameters
        ----------
        search_dict : dict
            Dictionary of DataFrames from get_all_search_outputs.
        kwargs : dict, optional 
            Can temporarily overwrite self flatten_output argument.
        
        Returns
        -------
        metadata_dict : OrderedDict
            OrderedDict of DataFrames with metadata for each query.
            Order matches the order of search_dict.
        """

        object_path_dict = {query: df.id.convert_dtypes().tolist()
                            for query, df in search_dict.items()}
        
        metadata_dict = super().get_all_metadata(
            object_path_dict=object_path_dict,
            **kwargs
        )

        return metadata_dict


class ZenodoScraper(AbstractTermScraper):
    """Scrapes the Zenodo API for all data relating to the given search terms.

    Parameters
    ----------
    search_terms : list-like, optional
        Terms to search over. Can be (re)set via set_search_terms() or passed in
        directly to search functions.
    flatten_output : boolean, optional (default=None)
        Flag for specifying if nested output should be flattened. Can be passed
        in directly to functions to override set parameter.
    credentials : str, optional
        API token or pkl filepath containing credentials in dict.
        If filepath, data in file must be formatted as a dictionary of the form
        data_dict['{REPO_NAME}_TOKEN']: MY_KEY, or as a string containing the 
        key.
    """

    def __init__(self, search_terms=None, flatten_output=None,
                 credentials=None):
        super().__init__('zenodo', search_terms, flatten_output, credentials)
        self.base_url = 'https://zenodo.org/api/records'

    @staticmethod
    def accept_user_credentials():
        return True

    def get_individual_search_output(self, search_term, **kwargs):
        """Returns information about all records from Zenodo.

        Parameters
        ----------
        search_term : str
        kwargs : dict, optional
            Can temporarily overwrite self flatten_output argument.

        Returns
        -------
        search_df : pandas.DataFrame
        """

        # Make sure out input is valid
        assert isinstance(search_term, str), 'Search term must be a string'

        flatten_output = kwargs.get('flatten_output', self.flatten_output)

        # Set search variables
        search_year = 2021
        search_df = pd.DataFrame()
        start_date = f'{search_year}-01-01'
        end_date = f'{search_year}-12-31'

        search_params = {'q': f'{search_term} AND created:[{start_date} TO {end_date}]',
                         'page': 1,
                         'size': 1000}

        # Run initial search & extract output
        print(f'Searching {search_year}')
        self._print_progress(search_params['page'])
        response, output = self.get_request_output(self.base_url, params=search_params)

        # Gather high-level search information from the 'aggregations' entry
        search_aggregation_info = output['aggregations']

        # Loop over search years
        # searches until the current search year does not return any results
        while output.get('hits').get('total'):
            # Loop over pages - searches until the current page is empty
            while response.status_code == 200 and output.get('hits').get('hits'):
                output = output['hits']['hits']
                # Flatten output
                if flatten_output:
                    output = [flatten(result) for result in output]

                # Turn outputs into DataFrame & add to cumulative search df
                output_df = pd.DataFrame(output)
                output_df['page'] = search_params['page']

                search_df = pd.concat(
                    [search_df, output_df]).reset_index(drop=True)

                # Increment page for next search
                search_params['page'] += 1
                
                # Run search & extract output
                self._print_progress(search_params['page'])
                response, output = self.get_request_output(self.base_url, params=search_params)

            # Change search year, reset search page
            search_year -= 1
            start_date = f'{search_year}-01-01'
            end_date = f'{search_year}-12-31'

            search_params['q'] = f'{search_term} AND created:[{start_date} TO {end_date}]'
            search_params['page'] = 1

            # Run search & extract output
            print(f'Searching {search_year}')
            self._print_progress(search_params['page'])
            response, output = self.get_request_output(self.base_url, params=search_params)

        return search_df
