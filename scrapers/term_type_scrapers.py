import itertools
import json
import os
import re
import sys
from abc import ABC, abstractmethod
from collections import OrderedDict

import pandas as pd
import requests
import selenium.webdriver.support.expected_conditions as EC
from bs4 import BeautifulSoup
from flatten_json import flatten
from kaggle import KaggleApi
from kaggle.rest import ApiException
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.select import By
from selenium.webdriver.support.wait import WebDriverWait
from tqdm import tqdm
from webdriver_manager.chrome import ChromeDriverManager

from scrapers.base_scrapers import AbstractAPIScraper, AbstractWebScraper

sys.path.append('..')

class AbstractTermTypeScraper(AbstractAPIScraper):
    """Base Class for scraping repository API's based on search term and type.

    Parameters
    ----------
    repository_name : str
        Name of the repository being scraped. Used for loading credentials and
        saving output results.
    search_terms : list-like, optional (default=None)
        Terms to search over. Can be (re)set via set_search_terms() or passed in
        directly to search functions to override set parameter.
    search_types : list-like, optional (default=None)
        Data types to search over. Can be (re)set via set_search_types() or 
        passed in directly to search functions to override set parameter.
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
            search_types=None,
            flatten_output=False,
            credentials=None
    ):
        super().__init__(repository_name, flatten_output, credentials)

        if search_terms:
            self.set_search_terms(search_terms)

        if search_types:
            self.set_search_types(search_types)

    def set_search_terms(self, search_terms):
        self.search_terms = search_terms

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
            Can temporarily overwrite self attributes and accept save params.

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
        """Queries the API for each search term/type combination.

        Parameters
        ----------
        kwargs : dict, optional
            Can temporarily overwrite self search_terms, search_types, and 
            flatten_output arguments.

        Returns
        -------
        search_dict : OrderedDict of DataFrames
            Stores the results of each call to get_individual_search_output in 
            the form search_dict[(search_term, search_type)] = df.
        """

        # Set method variables if different than default
        search_terms = kwargs.get('search_terms', self.search_terms)
        search_types = kwargs.get('search_types', self.search_types)
        flatten_output = kwargs.get('flatten_output', self.flatten_output)

        search_dict = OrderedDict()

        for search_term, search_type in itertools.product(search_terms, search_types):
            print(f'Searching {search_term} {search_type}.')
            search_dict[(search_term, search_type)] = \
                self.get_individual_search_output(
                    search_term=search_term, 
                    search_type=search_type, 
                    flatten_output=flatten_output
                )
            print('Search completed.', flush=True)

        return search_dict

    @abstractmethod
    def get_individual_search_output(self, search_term, search_type, **kwargs):
        pass

    def get_all_metadata(self, object_path_dict, **kwargs):
        """Retrieves all metadata that relates to the provided DataFrames.
        
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
            search_term, search_type = query
            print(f'Querying {search_term} {search_type} metadata.')

            metadata_dict[query] = self.get_query_metadata(
                    object_paths=object_paths, 
                    flatten_output=flatten_output
                )
        
        return metadata_dict

    def get_query_metadata(self, object_paths, **kwargs):
        # We raise an error instead of requiring implementation via 
        # @abstractmethod since not all derived classes may require it
        raise NotImplementedError

class DataverseScraper(AbstractTermTypeScraper, AbstractWebScraper):
    """Scrapes the Dataverse API for all data relating to the given search params.

    Parameters
    ----------
    path_file : str
        Json file for loading path dict.
        Must be of the form {search_type: {path_type: path_dict}}
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
        If mode needs to be specified, must be done via load_credentials 
        function.
    """

    search_type_options = ('dataset', 'file')

    def __init__(
        self,
        path_file,
        search_terms=None,
        search_types=None,
        flatten_output=None,
        credentials=None,
    ):
    
        # Create driver
        os.environ['WDM_LOG_LEVEL'] = '0'
        chrome_options = Options()
        chrome_options.add_argument('--headless')

        driver = webdriver.Chrome(
                ChromeDriverManager(print_first_line=False).install(), 
                options=chrome_options
            )

        AbstractTermTypeScraper.__init__(
            self,
            repository_name='dataverse', 
            search_terms=search_terms,
            search_types=search_types, 
        )

        AbstractWebScraper.__init__(
            self,
            repository_name='dataverse',
            driver=driver,
            path_file=path_file,
            flatten_output=flatten_output
        )

        self.base_url = 'https://dataverse.harvard.edu/api'
        self.file_url = 'https://dataverse.harvard.edu/file.xhtml?fileId='
        self.headers = dict()

        # Set credentials (was not set via parent init due to overloading)
        if credentials:
            self.load_credentials(credentials=credentials)

    @staticmethod
    def accept_user_credentials():
        return True

    def load_credentials(self, credentials='credentials.pkl', **kwargs):
        """Load the credentials given filepath or token.

        Parameters
        ----------
        credentials : str, optional (default=credentials.pkl)
            API token or pkl filepath containing credentials in dict.
            If pkl filepath, data in file must be formatted as a dictionary of 
            the form data_dict['{REPO_NAME}_TOKEN']: MY_KEY, or as a string 
            containing the key.
        kwargs : dict, optional
            Possible additional arguments include:
            file_format : str
                Choice of format for opening pkl file. Choices include the
                'mode' parameters for the python open() function. If none is 
                provided, files with attempt to open via 'rb'.
        """

        super().load_credentials(credentials=credentials, **kwargs)
        self.headers['X-Dataverse-key'] = self.credentials
    
    def get_individual_search_output(self, search_term, search_type, **kwargs):
        """Scrapes Dataverse API for the specified search term and type.

        Parameters
        ----------
        search_term : str
        search_type : str
        kwargs : dict, optional
            Can temporarily overwrite self flatten_output argument.

        Returns
        -------
        search_df : DataFrame
        """

        flatten_output = kwargs.get('flatten_output', self.flatten_output)
        search_url = f'{self.base_url}/search'

        # Validate input
        if not isinstance(search_term, str):
            raise ValueError('Search term must be a string.')
        if search_type not in DataverseScraper.search_type_options:
            raise ValueError('Can only search dataset, file.')

        # Set search parameters
        start = 0
        page_size = 100
        search_df = pd.DataFrame()
        page_idx = 0

        search_params = {
            'q': search_term,
            'per_page': page_size,
            'start': start,
            'type': search_type
        }

        # Conduct initial search & extract results
        self._print_progress(page_idx)
        _, output = self.get_request_output(search_url, 
                                                   params=search_params,
                                                   headers=self.headers)
        output = output['data']

        # Search until no more items are returned
        while output.get('items'):
            output = output['items']

            # Flatten results if necessary
            if flatten_output:
                output = [flatten(result) for result in output]
            
            output_df = pd.DataFrame(output)
            output_df['page'] = (search_params['start'] // 
                                 search_params['per_page'] + 1)

            search_df = pd.concat([search_df, output_df]).reset_index(drop=True)

            # Increment result offset to perform another search
            search_params['start'] += search_params['per_page']
            page_idx += 1

            # Perform next search and convert results to json
            self._print_progress(page_idx)
            _, output = self.get_request_output(search_url, 
                                                params=search_params,
                                                headers=self.headers)
            output = output['data']
        
        if not search_df.empty:
            # Modify file link for metadata search
            if search_type == 'file':
                search_df['download_url'] = search_df['url']
                search_df['url'] = search_df.apply(
                    lambda row: f'{self.file_url}{row.file_id}',
                    axis=1
                )

            return search_df
        else:
            return None

    def _get_attribute_value(self, soup, path):
        try:
            return soup.select_one(path).text
        except AttributeError:
            return None

    def _get_attribute_values(self, **kwargs):
        """Returns attribute values for all relevant given attribute path dicts.
        
        Parameters
        ----------
        kwargs : dict, optional
            Attribute dicts to parse through. Accepts landing page, metadata, 
            and terms dicts.
        
        Returns
        -------
        attribute_value_dict : dict
        """

        attribute_value_dict = dict()

        # Extract attribute path dicts
        landing_attribute_paths = kwargs.get('landing_attribute_paths')
        metadata_attribute_paths = kwargs.get('metadata_attribute_paths')
        terms_attribute_paths = kwargs.get('terms_attribute_paths')
        
        if landing_attribute_paths:
            # Retrieve html data and create parsable object
            soup = self._get_soup(features='html.parser')
            
            landing_attribute_values = {
                attribute: self._get_attribute_value(soup, path) 
                    for attribute, path in landing_attribute_paths.items()
            }
            attribute_value_dict = {**attribute_value_dict, 
                                    **landing_attribute_values}
        if metadata_attribute_paths:
            try:
                self.driver.find_element_by_link_text('Metadata').click()
                
                # Retrieve html data and create parsable object
                soup = self._get_soup(features='html.parser')
                
                metadata_attribute_values = {
                    attribute: self._get_attribute_value(soup, path) 
                        for attribute, path in metadata_attribute_paths.items()
                }
                attribute_value_dict = {**attribute_value_dict, 
                                        **metadata_attribute_values}
            except:
                print(self.driver.title, 'metadata')
        if terms_attribute_paths:
            try:
                self.driver.find_element_by_link_text('Terms').click()

                # Retrieve html data and create parsable object
                soup = self._get_soup(features='html.parser')
                
                terms_attribute_values = {
                    attribute: self._get_attribute_value(soup, path) 
                        for attribute, path in terms_attribute_paths.items()
                }
                attribute_value_dict = {**attribute_value_dict, 
                                        **terms_attribute_values}
            except:
                print(self.driver.title, 'terms')
            
        return attribute_value_dict

    def _clean_results(self, results):
        """Cleans the results scraped from the page.
        
        Parameters
        ----------
        results : dict
        
        Returns
        -------
        results : dict
        """
        
        num_downloads = results.get('num_downloads')

        if num_downloads:
            results['num_downloads'] = re.findall('\d+', num_downloads)[0]
            
        return results   

    def get_query_metadata(self, object_paths, **kwargs):
        """
        Retrieves the metadata for the object/objects listed in object_paths.
        
        Parameters
        ----------
        object_paths : str/list-like
            String or list of strings containing the paths for the objects.
        kwargs : dict, optional
            Holds attribute paths for scraping metadata.
        
        Returns
        -------
        metadata_df : DataFrame
            DataFrame containing metadata for the requested objects.
        """

        # Validate input
        object_paths = self.validate_metadata_parameters(object_paths)
        
        # Create empty pandas dataframe to put results in
        metadata_df = pd.DataFrame()

        # Get details for each object
        for object_path in tqdm(object_paths):
            object_dict = dict()

            # Retrieve webpage
            self.driver.get(object_path)

            # Extract & clean attribute values
            object_dict = self._get_attribute_values(**kwargs)
            object_dict['url'] = object_path
            object_dict = self._clean_results(object_dict)

            # Add results to DataFrame
            metadata_df = metadata_df.append(object_dict, ignore_index=True)
            
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
            Order matches the order of search_output_dict.
        """

        metadata_dict = OrderedDict()

        for query, df in search_dict.items():
            if df is not None:
                search_term, search_type = query
                print(f'Querying {search_term} {search_type} metadata.')

                _, search_type = query
                object_paths = df['url']
            
                metadata_dict[query] = \
                    self.get_query_metadata(
                        object_paths, 
                        **self.path_dict[search_type]
                    )

        return metadata_dict


class FigshareScraper(AbstractTermTypeScraper):
    """Scrapes Figshare API for all data relating to the given search params.

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
        If mode needs to be specified, must be done via load_credentials 
        function.
    """

    search_type_options = ('articles', 'collections', 'projects')

    def __init__(
        self,
        search_terms=None,
        search_types=None,
        flatten_output=None,
        credentials=None
    ):
        super().__init__(repository_name='figshare', search_terms=search_terms,
                         search_types=search_types,
                         flatten_output=flatten_output, credentials=None)
        self.base_url = 'https://api.figshare.com/v2'
        self.merge_on = 'id'
        self.headers = dict()

        if credentials:
            self.load_credentials(credentials=credentials)

    @staticmethod
    def accept_user_credentials():
        return True

    def load_credentials(self, credentials='credentials.pkl', **kwargs):
        """Load the credentials given filepath or token.

        Parameters
        ----------
        credentials : str, optional (default=credentials.pkl)
            API token or pkl filepath containing credentials in dict.
            If pkl filepath, data in file must be formatted as a dictionary of 
            the form data_dict['{REPO_NAME}_TOKEN']: MY_KEY, or as a string 
            containing the key.
        kwargs : dict, optional
            Possible additional arguments include:
            file_format : str
                Choice of format for opening pkl file. Choices include the
                'mode' parameters for the python open() function. If none is 
                provided, files with attempt to open via 'rb'.
        """

        super().load_credentials(credentials=credentials, **kwargs)
        self.headers['Authorization'] = f'token {self.credentials}'

    def get_individual_search_output(self, search_term, search_type, **kwargs):
        """Calls the Figshare API for the specified search term and type.

        Parameters
        ----------
        search_term : str
        search_type : str
        kwargs : dict, optional
            Can temporarily overwrite self flatten_output argument.

        Returns
        -------
        search_df : DataFrame
        """

        flatten_output = kwargs.get('flatten_output', self.flatten_output)

        # Validate input
        if not isinstance(search_term, str):
            raise ValueError('Search term must be a string.')
        if search_type not in FigshareScraper.search_type_options:
            raise ValueError('Can only search articles, collections, projects.')

        # Set search variables
        start_page = 1
        page_size = 1000
        output = None
        search_df = pd.DataFrame()
        search_year = 1950
        search_date = f'{search_year}-01-01'

        search_params = {
            'search_for': search_term,
            'published_since': search_date,
            'order_direction': 'asc',
            'page': start_page,
            'page_size': page_size
        }

        search_url = f'{self.base_url}/{search_type}'

        # Conduct initial search
        self._print_progress(search_params['page'])
        response, output = self.get_request_output(search_url, search_params,
                                                   self.headers)

        # Search as long as page is valid
        while response.status_code == 200:
            while response.status_code == 200 and output:
                # Flatten output if needed
                if flatten_output:
                    output = [flatten(result) for result in output]

                # Convert output to df & add query info
                output_df = pd.DataFrame(output)
                output_df['search_page'] = search_params['page']
                output_df['publish_query'] = search_params['published_since']

                # Append modified output df to cumulative df
                search_df = pd.concat([search_df, output_df]).reset_index(drop=True)

                # Increment page number to query over
                search_params['page'] += 1

                # Conduct search
                self._print_progress(search_params['page'])
                response, output = self.get_request_output(search_url, 
                                                           search_params,
                                                           self.headers)
            try:
                # If we did not get a full page of results, searching is complete
                if output_df.shape[0] < search_params['page_size']:
                    return search_df
            # If there's no output_df (no search results), return None
            except UnboundLocalError:
                return None
            
            # Get new date to search
            search_date = search_df['published_date'].values[-1].split('T')[0]
            search_params['published_since'] = search_date
            search_params['page'] = start_page

            # Conduct search
            self._print_progress(search_params['page'])
            response, output = self.get_request_output(search_url, search_params, 
                                                       self.headers)

        return search_df

    def get_query_metadata(self, object_paths, **kwargs):
        """
        Retrieves the metadata for the object/objects listed in object_paths.
        
        Parameters
        ----------
        object_paths : str/list-like
            string or list of strings containing the paths for the objects.
        kwargs : dict, optional
            Can temporarily overwrite self flatten_output argument.
        
        Returns
        -------
        metadata_df : DataFrame
            DataFrame containing metadata for the requested objects.
        """
        
        flatten_output = kwargs.get('flatten_output', self.flatten_output)

        # Validate input
        object_paths = self.validate_metadata_parameters(object_paths)
        
        # Create empty pandas dataframe to put results in
        metadata_df = pd.DataFrame()

        # Get details for each object
        for object_path in tqdm(object_paths):
            # Download the metadata
            _, json_data = self.get_request_output(url=object_path, 
                                                   headers=self.headers)

            # Flatten ouput, if necessary
            if flatten_output:
                json_data = flatten(json_data)

            metadata_df = metadata_df.append(json_data, ignore_index=True)
            
        return metadata_df

    def get_all_metadata(self, search_dict, **kwargs):
        """Retrieves all metadata that relates to the provided DataFrames.
        
        Parameters
        ----------
        search_dict : dict
            Dictionary of DataFrames from get_all_search_outputs.
        kwargs : dict, optional 
            Can temporarily overwrite self flatten_output argument.
        
        Returns:
        metadata_dict : OrderedDict
            OrderedDict of DataFrames with metadata for each query.
            Order matches the order of search_output_dict.
        """

        flatten_output = kwargs.get('flatten_output', self.flatten_output)

        object_path_dict = OrderedDict()

        for query, df in search_dict.items():
            if df is not None:
                _, search_type = query
                object_ids = df.id.convert_dtypes().tolist()
                object_paths = [f'{self.base_url}/{search_type}/{object_id}' for 
                                object_id in object_ids]
                
                object_path_dict[query] = object_paths
        
        metadata_dict = super().get_all_metadata(object_path_dict, 
                                                 flatten_output=flatten_output)

        return metadata_dict


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

        super().__init__(repository_name='kaggle', search_terms=search_terms,
                         search_types=search_types,
                         flatten_output=flatten_output)

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
            search_df = search_df.rename(columns={'id': 'datasetId', 'ref': 'id'})
            
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
            if (isinstance(e, ApiException) and e.status != 404 and
                'bigquery' not in e.headers['Turbolinks-Location']):
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
            json_data = self._retrieve_object_json(object_path, 
                                                   flatten_output=flatten_output)
            
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

        metadata_dict = super().get_all_metadata(object_path_dict, 
                                                 flatten_output=flatten_output)
        
        return metadata_dict


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
    credentials : str, optional
        API token or pkl filepath containing credentials in dict.
        If filepath, data in file must be formatted as a dictionary of the form
        data_dict['{REPO_NAME}_TOKEN']: MY_KEY, or as a string containing the 
        key.
    """

    search_type_options = ('conferences', 'datasets', 'evaluations', 'papers', 
                           'tasks')

    def __init__(
        self,
        search_terms=None,
        search_types=None,
        flatten_output=None,
        credentials=None
    ):
        super().__init__(repository_name='paperswithcode',
                         search_terms=search_terms,
                         search_types=search_types,
                         flatten_output=flatten_output,
                         credentials=credentials)
        self.base_url = 'https://paperswithcode.com/api/v1'

    @staticmethod
    def accept_user_credentials():
        return True

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
            self._print_progress(search_params['page'])
        response, output = self.get_request_output(search_url, params=search_params)

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

            search_df = pd.concat([search_df, output_df]
                                  ).reset_index(drop=True)

            # Increment page for search
            search_params['page'] += 1

            # Conduct a search
            if print_progress:
                self._print_progress(search_params['page'])
            response = requests.get(search_url, params=search_params)

            # Ensure we've received results if they exist
            # 200: OK, 404: page not found (no more results)
            while response.status_code not in [200, 404]:
                print(f'Search error "{response.status_code}" on page {search_params["page"]}')
                search_params['page'] += 1

                # Conduct a search
                if print_progress:
                    self._print_progress(search_params['page'])
                response = requests.get(search_url, params=search_params)

            output = response.json()

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
            raise ValueError(f'Search type must be one of {PapersWithCodeScraper.search_type_options}')

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
            return ['methods', 'repositories', 'results', 'tasks']
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
            print(f'Querying {metadata_type}.')

            for object_path in tqdm(object_paths):
                search_url = f'{self.base_url}/{search_type}/{object_path}/{metadata_type}'
                search_params = {'page': 1}

                # Conduct the search and add supplementary info to DataFrame
                object_df = self._conduct_search_over_pages(search_url,
                                                            search_params,
                                                            flatten_output)
                
                if object_df is not None:
                    object_df['id'] = object_path
                    object_df['page'] = search_params['page']

                # Merge with the cumulative search DataFrame
                search_df = pd.concat(
                    [search_df, object_df]).reset_index(drop=True)

            if not search_df.empty:
                metadata_dict[metadata_type] = search_df

        return metadata_dict

    def get_all_metadata(self, search_dict, **kwargs):
        """Retrieves all of the metadata that relates to the provided DataFrames.

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
                print(f'Querying {search_term} {search_type} metadata.')
                
                object_paths = df.id.values

                metadata_dict[query] = self.get_query_metadata(
                    object_paths=object_paths,
                    search_type=search_type,
                    **kwargs
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