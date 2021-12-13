import itertools
import json
import os
import pickle
import re
import time
from abc import ABC, abstractmethod, abstractstaticmethod
from collections import OrderedDict

import pandas as pd
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager


class AbstractScraper(ABC):
    """"
    Contains basic functions that are relevant for all scrapers.

    Parameters
    ----------
    repository_name : str
        Name of the repository being scraped. Used for loading credentials and
        saving output results.
    flatten_output : boolean, optional (default=False)
        Flag for specifying if nested output should be flattened. Can be passed
        in directly to functions to override set parameter.
    """

    def __init__(
        self, 
        repository_name,
        flatten_output=False
    ):
        self.repository_name = repository_name
        self.flatten_output = flatten_output

    def get_repo_name(self):
        return self.__class__.__name__.replace('Scraper', '')

    @abstractstaticmethod
    def accept_user_credentials():
        pass

    def _print_progress(self, page):
        print(f'Searching page {page}', end='\r', flush=True)

    def parse_numeric(self, entry):
        """Returns a list of numeric substrings."""
        return re.findall(r'\d+', entry)

    def _convert_key_to_str(self, key):
        if isinstance(key, str):
            return key
        else:
            return '_'.join(key)

    def _validate_save_filename(self, filename):
        """Removes quotations from filename, replaces spaces with underscore."""

        return filename.replace('"', '').replace("'", '').replace(' ', '_')

    def save_dataframes(self, results, data_dir):
        # Create output dir if not already present
        if not os.path.isdir(data_dir):
            os.makedirs(data_dir)
        
        # Save each dataframe
        for query, df in results.items():
            # Set save file
            if isinstance(query, str):
                output_filename = f'{query}.json'
            else:
                search_term, search_type = query
                output_filename = f'{search_term}_{search_type}.json'
            
            # Make sure filename is safe for all systemes
            output_filename = self._validate_save_filename(output_filename)

            # Save results
            self.save_results(
                results=df, 
                filepath=os.path.join(data_dir, output_filename)
            )

    def save_results(self, results, filepath):
        """Saves the specified results to the file provided.

        Parameters
        ----------
        results : DataFrame
            If DataFrame, results will be stored in a csv format.
        filepath : str
            Location to store file in. Take note of output type as specified
            above, as appending the incorrect filetype may result in the file
            being unreadable.
        """

        if isinstance(results, pd.DataFrame):
            results.to_json(filepath)
        else:
            raise ValueError(f'Can only save DataFrame, not {type(results)}')

class AbstractWebScraper(AbstractScraper):
    """Base class for all repository web scrapers. 

    Contains basic functions that are relevant for all derived classes.

    Parameters
    ----------
    repository_name : str
        Name of the repository being scraped. Used for loading credentials and
        saving output results.
        Web scrapers do not require user credentials at all.
    driver : WebDriver
        Selenium webdriver that is used for querying webpages to be scraped.
    path_file : str
        Json file for loading path dict.
        Must be of the form {'path_dict': path_dict}
    flatten_output : boolean, optional (default=False)
        Flag for specifying if nested output should be flattened. Can be passed
        in directly to functions to override set parameter.
    """

    def __init__(self, repository_name, path_file, flatten_output=False):
        AbstractScraper.__init__(
            self,
            repository_name=repository_name, 
            flatten_output=flatten_output
        )

        # Create driver
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        os.environ['WDM_LOG_LEVEL'] = '0'

        self.driver = webdriver.Chrome(
            ChromeDriverManager(print_first_line=False).install(), 
            options=chrome_options
        )

        with open(path_file) as f:
            self.path_dict = json.load(f)
    
    def find_first_match(self, entry, pattern):
        """Returns the first match of a regex pattern"""
        try:
            return re.search(pattern, entry).group()
        except:
            return None

    def _get_soup(self, **kwargs):
        html = self.driver.page_source
        return BeautifulSoup(html, **kwargs)

    def _get_single_attribute(self, soup, path):
        try:
            return soup.select_one(path)
        except AttributeError:
            return None

    def get_single_attribute_value(
        self, 
        soup, 
        path=None, 
        class_type=None,
        id_=None,
        err_return=None
    ):
        """Retrieves the requested value from the soup object.
        
        For a page attribute with a single value 
        ('abstract', 'num_instances', etc), returns the value. 

        Either a full CSS Selector Path must be passed via 'path', or an HTML
        class and CSS ID path must be passed via 'class_type' and 'id_', 
        respectively.

        For attributes with potentially multiple values, such as 'keywords', 
        use get_variable_attribute_values(...)
        
        Parameters
        ----------
        soup : BeautifulSoup
            BeautifulSoup object containing the html to be parsed.
        path : str, optional (default=None)
            CSS Selector Path for attribute to scrape.
            If None:

        class_type : str, optional (default=None)
            HTML class type to find. Must be passed along with id_.
        id_ : optional (default=None)
            Value or pattern to pass to bs4.find() id argument. Must be passed
            along with class_type.
        err_return : optional (default=None)
            Value to return in case of error.

        Returns
        -------
        value of attribute
        """
        if path:
            try:
                return self._get_single_attribute(soup, path).text
            except:
                return err_return
        elif class_type and id_:
            try:
                return soup.find(class_type, id=id_).text
            except:
                return err_return
        else:
            raise ValueError('Must pass a path or class type and id to get.')

    def get_variable_attribute_values(self, soup, path):
        """Retrieves the requested value from the soup object.
        
        For a page attribute with potentially multiple values, such as 
        'keywords', return the values as a list. For attributes with a single 
        value, such as 'abstract', use get_single_attribute_value(...)
        
        Parameters
        ----------
        soup : BeautifulSoup
            BeautifulSoup object containing the html to be parsed.
        path : str
            CSS Selector Path for attribute to scrape.
        
        Returns
        -------
        list
            Value(s) of attribute.
        """ 
        try:
            return [tag.text for tag in soup.select(path)]
        except:
            print(soup.title, path)


class AbstractAPIScraper(AbstractScraper):
    """Base class for all repository API scrapers. 

    Contains basic functions that are relevant for all derived classes.

    Parameters
    ----------
    repository_name : str
        Name of the repository being scraped. Used for loading credentials and
        saving output results.
        Web scrapers do not require user credentials at all.
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
        flatten_output=False, 
        credentials=None
    ):
        AbstractScraper.__init__(
            self,
            repository_name=repository_name, 
            flatten_output=flatten_output
        )

        if credentials:
            self.load_credentials(credentials=credentials)

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
        
        if not isinstance(credentials, str):
            raise ValueError('Credential value must be of type str')

        # Try to load credentials from file
        if os.path.exists(credentials):
            file_format = kwargs.get('file_format', 'rb')

            with open(credentials, file_format) as credential_file:
                credential_file_data = pickle.load(credential_file)

                if isinstance(credential_file_data, (dict, OrderedDict)):
                    try:
                        token_name = f'{self.repository_name.upper()}_TOKEN'
                        self.credentials = credential_file_data[token_name]
                    except KeyError:
                        print(f'{token_name} not found. Attempting to run unverified...')
                        self.credentials = ''
                elif isinstance(credential_file_data, str):
                    self.credentials = credential_file_data
                else:
                    raise ValueError(f'Invalid credential data in pkl file')

        # Set credentials from string value
        else:
            self.credentials = credentials


    def validate_metadata_parameters(self, object_paths):
        """Ensures that the metadata object paths are of the proper form.

        Parameters
        ----------
        object_paths : str/list-like

        Returns
        -------
        object_paths : str/list-like
        """

        # If a single object path is provided as a string, need to wrap as list
        if isinstance(object_paths, str):
            object_paths = [object_paths]

        # Ensure input is not empty
        if len(object_paths) == 0:
            raise ValueError('Cannot perform search without object paths')

        return object_paths


    def get_request_output(self, url, params=None, headers=None):
        """Performs a requests.get(...) call, returns response and json.

        Parameters
        ----------
        url : str
        params : dict, optional (default=None)
            Params to pass to requests.get().
        headers : dict, optional (default=None)
            Headers to pass to requests.get().

        Returns
        -------
        r : response
        outpout : dict
            Json object from r(esponse).
        """

        r = requests.get(url, params=params, headers=headers)
        try:
            output = r.json()
        except json.decoder.JSONDecodeError as e:
            # 429: Rate limiting (wait and then try the request again)
            if r.status_code == 429:
                # Wait until we can make another request
                reset_time = int(r.headers['RateLimit-Reset'])
                current_time = int(time.time())
                time.sleep(reset_time - current_time)

                r, output = self.get_request_output(
                    url=url, 
                    params=params, 
                    headers=headers
                )
            else:
                print(vars(r))
                raise e

        return r, output

    def merge_search_and_metadata_dicts(
        self, 
        search_dict, 
        metadata_dict,
        on=None, 
        left_on=None, 
        right_on=None,
        **kwargs
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
            Placeholder argument to allow interitence overloading.

        Returns
        -------
        df_dict : OrderedDict
            OrderedDict containing all of the merged search/metadata dicts.
        """

        # Merge the DataFrames
        df_dict = OrderedDict()
        for query_key in search_dict.keys():
            search_df = search_dict[query_key]

            # If the search DataFrame has matching metadata, merge
            if query_key in metadata_dict:
                metadata_df = metadata_dict[query_key]
                df_all = pd.merge(
                    search_df.convert_dtypes(), 
                    metadata_df.convert_dtypes(), 
                    on=on,
                    left_on=left_on, 
                    right_on=right_on, 
                    how='outer',
                    suffixes=('_search', '_metadata')
                )
            # If no metadata, just add the search_df
            else:
                df_all = search_df

            df_dict[query_key] = df_all

        return df_dict

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
                search_term, 
                flatten_output=flatten_output
            )
            print('Search completed.', flush=True)

        return search_dict

    @abstractmethod
    def get_individual_search_output(self, search_term, **kwargs):
        print('abstract metadata')

    def get_all_metadata(self, object_path_dict, **kwargs):
        """Retrieves all metadata related to the provided DataFrames.
        
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
                object_paths, 
                flatten_output=flatten_output
            )
        
        return metadata_dict

    def get_query_metadata(self, object_paths, **kwargs):
        # We raise an error instead of requiring implementation via
        # @abstractmethod since not all derived classes will require it
        raise NotImplementedError

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
