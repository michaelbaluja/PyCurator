import os
import pickle
import sys
from abc import ABC, abstractmethod
from collections import OrderedDict

import openml
import pandas as pd
import requests
import selenium.webdriver.support.expected_conditions as EC
from bs4 import BeautifulSoup
from flatten_json import flatten
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.select import By
from selenium.webdriver.support.wait import WebDriverWait
from tqdm import tqdm
from webdriver_manager.chrome import ChromeDriverManager

from utils import flatten_nested_df

from .base_scrapers import AbstractAPIScraper, AbstractWebScraper

sys.path.append('..')


class AbstractTypeScraper(AbstractAPIScraper):
    """Base Class for scraping repository API's based on search type.

    Parameters
    ----------
    repository_name : str
        Name of the repository being scraped. Used for loading credentials and
        saving output results.
        Web scrapers do not require user credentials at all.
    search_types : list-like, optional (default=None)
        types to search over. Can be (re)set via set_search_types() or passed in
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
        search_types=None,
        flatten_output=False,
        credentials=None
    ):
        super().__init__(repository_name, flatten_output, credentials)

        if search_types:
            self.set_search_types(search_types)

    def set_search_types(self, search_types):
        self.search_types = search_types

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
            metadata_dict = self.get_all_metadata(search_dict=search_dict, **kwargs)
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
        """Queries the API for each search type.

        Parameters
        ----------
        kwargs : dict, optional
            Can temporarily overwrite self search_types and flatten_output 
            arguments.

        Returns
        -------
        search_dict : OrderedDict of DataFrames
            Stores the results of each call to get_individual_search_output in
            the form search_output_dict[{search_type}] = df.
        """

        # Set method variables if different than default
        search_types = kwargs.get('search_types', self.search_types)
        flatten_output = kwargs.get('flatten_output', self.flatten_output)

        search_dict = OrderedDict()

        for search_type in search_types:
            print(f'Searching {search_type}.')
            search_dict[search_type] = self.get_individual_search_output(
                search_type,
                flatten_output=flatten_output
            )
            print('Search completed.', flush=True)

        return search_dict

    @abstractmethod
    def get_individual_search_output(self, search_type, **kwargs):
        pass

    def get_query_metadata(self, object_paths, search_type, **kwargs):
        # We raise an error instead of requiring implementation via
        # @abstractmethod since not all derived classes will require it
        raise NotImplementedError


class OpenMLScraper(AbstractTypeScraper, AbstractWebScraper):
    """Scrapes the OpenML API for all data relating to the given search types.

    Parameters
    ----------
    path_file : str
        Json file for loading path dict.
        Must be of the form {search_type: {path_type: path_dict}}
    search_types : list-like, optional
        Types to search over. Can be (re)set via set_search_types() or passed in
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

    search_type_options = ('datasets', 'runs', 'tasks', 'evaluations')

    def __init__(
        self,
        path_file,
        search_types=None,
        flatten_output=None,
        credentials=None
    ):

        # Create driver
        os.environ['WDM_LOG_LEVEL'] = '0'
        chrome_options = Options()
        chrome_options.add_argument('--headless')

        driver = webdriver.Chrome(
                ChromeDriverManager(print_first_line=False).install(), 
                options=chrome_options
            )

        # Initialize parent classes
        AbstractTypeScraper.__init__(
            self,
            repository_name='openml',
            search_types=search_types, 
        )

        AbstractWebScraper.__init__(
            self,
            repository_name='openml',
            driver=driver,
            path_file=path_file,
            flatten_output=flatten_output
        )

        self.base_url = 'https://dataverse.harvard.edu/api'

        if not openml.config.apikey:
            openml.config.apikey = credentials

    @staticmethod
    def accept_user_credentials():
        return True

    def _get_value_attributes(self, obj):
        """
        Given an object, returns a list of the object's value-based variables

        Parameters
        ----------
        obj : list-like 
            object to be analyzed 

        Returns
        -------
        attributes : list
            value-based variables for the object given
        """

        # This code will pull all of the attributes of the provided class that
        # are not callable or "private" for the class.
        return [attr for attr in dir(obj) if
                not hasattr(getattr(obj, attr), '__call__')
                and not attr.startswith('_')]

    def _get_evaluations_search_output(self, flatten_output):
        # Get different evaluation measures we can search for
        evaluations_measures = openml.evaluations.list_evaluation_measures()

        # Create DataFrame to store attributes
        evaluations_df = pd.DataFrame()

        # Get evaluation data for each available measure
        for measure in tqdm(evaluations_measures):
            # Query all data for a given evaluation measure
            evaluations_dict = openml.evaluations.list_evaluations(measure)

            try:
                # Grab one of the evaluations in order to extract attributes
                sample_evaluation = next(iter(evaluations_dict.items()))[1]
            # StopIteration will occur in the preceding code if an evaluation
            # search returns no results for a given measure
            except StopIteration:
                continue

            # Get list of attributes the evaluation offers
            evaluations_attributes = self._get_value_attributes(
                sample_evaluation)

            # Adds the queried data to the DataFrame
            for query in evaluations_dict.values():
                attribute_dict = {attribute: getattr(query, attribute) for
                                  attribute in evaluations_attributes}
                evaluations_df = evaluations_df.append(attribute_dict,
                                                       ignore_index=True)

            evaluations_df = flatten_nested_df(evaluations_df)

        return evaluations_df

    def get_dataset_related_tasks(self, data_df):
        """Queries the task and run information related to the provided datasets.

        Parameters
        ----------
        data_df : DataFrame

        Returns
        -------
        data_df : DataFrame
            Original input with task and run information appended.
        """

        for url in data_df['openml_url']:
            self.driver.get(url)
            soup = self._get_soup()

            for attribute, path in self.path_dict.items():
                data_df[attribute] = self.get_single_attribute_value(soup, path)

        return data_df

    def get_individual_search_output(self, search_type, **kwargs):
        """Returns information about all queried information types on OpenML.

        Parameters
        ----------
        search_type : str
        kwargs : dict, optional
            Can temporarily overwrite self flatten_output argument.

        Returns
        -------
        search_df : DataFrame
        """

        flatten_output = kwargs.get('flatten_output', self.flatten_output)

        # Ensure parameters are valid
        if search_type not in OpenMLScraper.search_type_options:
            raise ValueError(f'"{search_type}" is not a valid object type')

        # Handle special case for evaluations
        if search_type == 'evaluations':
            return self._get_evaluations_search_output(flatten_output)

        # Use query type to get necessary openml api functions
        base_command = getattr(openml, search_type)
        list_queries = getattr(base_command, f'list_{search_type}')

        # Get base info on every object listed on OpenML for the given query
        # Since there's too many runs to get all at once, we need to search
        # with offsets and rest periods so the server doesn't overload.

        # Set search params
        index = 0
        size = 10000
        search_df = pd.DataFrame()

        # Perform initial search
        self._print_progress(index)
        search_results = list_queries(offset=(index * size), size=size)

        # Serach until all queries have been returned
        while search_results:
            # Add results to cumulative output df
            output_df = pd.DataFrame(search_results).transpose()
            output_df['page'] = index + 1
            search_df = pd.concat([search_df, output_df]).reset_index(drop=True)

            # Increment search range
            index += 1

            # Perform next search
            self._print_progress(index)
            search_results = list_queries(offset=(index * size), size=size)

        # Flatten output (if necessary)
        if flatten_output:
            search_df = flatten_nested_df(search_df)

        if search_type == 'datasets':
            search_df = self.get_dataset_related_tasks(search_df)

        return search_df

    def get_query_metadata(self, object_paths, search_type, **kwargs):
        """Retrieves the metadata for the file/files listed in object_paths

        Parameters
        ----------
        object_paths : str/list-like
        search_type : str
        kwargs : dict, optional
            Can temporarily overwrite self flatten_output argument

        Returns
        -------
        metadata_df : DataFrame
        """

        flatten_output = kwargs.get('flatten_output', self.flatten_output)

        # Ensure object paths are of the proper form
        object_paths = self.validate_metadata_parameters(object_paths)

        base_command = getattr(openml, search_type)
        get_query = getattr(base_command, f'get_{search_type[:-1:]}')

        # Request each query
        queries = []
        error_queries = []
        for object_path in tqdm(object_paths):
            try:
                queries.append(get_query(object_path))
            except:
                error_queries.append(object_path)

        # Get list of metadata attributes the queries offer
        query_attributes = self._get_value_attributes(queries[0])

        # Create DataFrame to store metadata attributes
        metadata_df = pd.DataFrame(columns=query_attributes)

        # Append attributes of each dataset to the DataFrame
        for query in queries:
            attribute_dict = {attribute: getattr(query, attribute) for
                              attribute in query_attributes}
            metadata_df = metadata_df.append(attribute_dict, ignore_index=True)

        # Flatten the nested DataFrame
        if flatten_output:
            metadata_df = flatten_nested_df(metadata_df)

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

        metadata_dict = OrderedDict()

        for query, df in search_dict.items():
            print(f'Querying {query} metadata.')
            if query == 'datasets':
                id_name = 'did'
            elif query == 'runs':
                id_name = 'run_id'
            elif query == 'tasks':
                id_name = 'tid'

            # Grab the object paths as the id's from the DataFrame
            object_paths = df[id_name].values

            metadata_dict[query] = self.get_query_metadata(
                object_paths=object_paths, 
                search_type=query,
                **kwargs
            )
            
        return metadata_dict
