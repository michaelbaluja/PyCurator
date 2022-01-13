import itertools
import json
import os
import pickle
import re
import time
from abc import ABC, abstractmethod, abstractstaticmethod
from collections import OrderedDict
import queue

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

        self.queue = queue.Queue()

    @classmethod
    def get_repo_name(cls):
        """Return the name of the class without the 'Scraper' suffix."""
        return cls.__name__.replace('Scraper', '')

    @abstractstaticmethod
    def accept_user_credentials():
        pass

    def _print_progress(self, page):
        """Update queue with current page being searched."""
        self.queue.put(f'Searching page {page}')

    def _validate_save_filename(self, filename):
        """Removes quotations from filename, replaces spaces with underscore."""
        return filename.replace('"', '').replace("'", '').replace(' ', '_')

    def save_dataframes(self, results, data_dir):
        """Export DataFrame objects to json file in specified directory.

        Parameters
        ----------
        results : dict
        data_dir : str
        """

        assert isinstance(results, dict)
        assert isinstance(data_dir, str)

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
        
        Raises
        ------
        ValueError
            If a non-dataframe object is passed.
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

        self.queue.put('Initializing WebDriver.')
        self.driver = webdriver.Chrome(
            ChromeDriverManager(print_first_line=False).install(), 
            options=chrome_options
        )

        # Load CSS Selector Path file
        if path_file:
            with open(path_file) as f:
                self.path_dict = json.load(f)

    def _get_soup(self, **kwargs):
        """Return a BeautifulSoup object for the object driver current page."""
        html = self.driver.page_source
        return BeautifulSoup(html, **kwargs)

    def _get_single_attribute_from_path(self, soup, path):
        return soup.select_one(path)
        
    def _get_single_attribute_from_tag_info(
        self, 
        soup, 
        class_type=re.compile(r''), 
        **kwargs
    ):
        """Find and return BeautifulSoup Tag from given specifications."""
        return soup.find(class_type, **kwargs)

    def _get_parent_attribute(
        self,
        soup,
        string,
    ):
        """Find BeautifulSoup Tag from given specifications, return parent."""
        attr = self._get_single_attribute_from_tag_info(
            soup=soup,
            string=string
        )
        try:
            parent_tag = attr.parent
        except AttributeError:
            parent_tag = None

        return parent_tag

    def _get_pibling_attributes(
        self,
        soup,
        string,
        **kwargs
    ):
        """Return the tag for the (p)arent tag's s(ibling) tags.

        Parameters
        ----------
        soup : bs4.BeautifulSoup
        string : str
            Pattern for locating tag of interest.
        **kwargs : dict, optional
            Additional parameters passed to the 
            bs4.element.Tag.find_next_siblings() call.
        
        Returns
        -------
        list

        See Also
        --------
        bs4.element.Tag.find_next_siblings()
        """

        parent = self._get_parent_attribute(soup, string)
        return parent.find_next_siblings(**kwargs)

    def _get_attribute_value(self, tag, err_return=None, **kwargs):
        """Return text for the provided Tag, queried with kwargs."""
        try:
            return tag.get_text(**kwargs)
        except AttributeError:
            return err_return

    def get_single_attribute(
        self, 
        soup, 
        path=None, 
        class_type=re.compile(r''),
        **find_kwargs
    ):
        """Retrieves the requested value from the soup object.
        
        For a page attribute with a single value 
        ('abstract', 'num_instances', etc), returns the value. 

        Either a full CSS Selector Path must be passed via 'path', or an HTML
        class and additional parameters must be passed via 'class_type' and 
        **find_kwargs, respectively.

        For attributes with potentially multiple values, such as 'keywords', 
        use get_variable_attribute_values(...)
        
        Parameters
        ----------
        soup : BeautifulSoup
            BeautifulSoup object containing the html to be parsed.
        path : str, optional (default=None)
            CSS Selector Path for attribute to scrape.
            If None:
                Search is performed using class_type and **find_kwargs.
        class_type : str, optional (default=re.compile(r''))
            HTML class type to find.
        **find_kwargs : dict, optional
            Additional arguments for 'soup.find()' call.

        Returns
        -------
        attr : bs4.element.Tag

        Raises
        ------
        ValueError
            If no CSS path or find_kwargs are passed.

        See Also
        --------
        re.compile : Compile a regular expression pattern into a regular
            expression object, which can be used for matching using re.search().
        """
        
        if path:
            attr = self._get_single_attribute_from_path(soup, path)
        elif find_kwargs:
            attr = self._get_single_attribute_from_tag_info(
                soup,
                class_type, 
                **find_kwargs
            )
        else:
            raise ValueError('Must pass a CSS path or find attributes.')

        return attr

    def get_variable_attribute(
        self, 
        soup, 
        path=None,
        class_type=re.compile(r''),
        **find_kwargs):
        """Retrieves the requested value from the soup object.
        
        For a page attribute with potentially multiple values, such as 
        'keywords', return the values as a list. For attributes with a single 
        value, such as 'abstract', use get_single_attribute_value(...)
        
        Parameters
        ----------
        soup : BeautifulSoup
            BeautifulSoup object containing the html to be parsed.
        path : str, optional (default=None)
            CSS Selector Path for attribute to scrape.
        class_type : str, optional (default=re.compile(r''))
            HTML class type to find.
        **find_kwargs : dict, optional
            Additional arguments for 'soup.find_all()' call.

        Returns
        -------
        attrs : list

        Raises
        ------
        ValueError
            If no CSS path or find_kwargs are passed.
        """ 

        if path:
            attrs = soup.select(path)
        elif find_kwargs:
            attrs = soup.find_all(class_type, **find_kwargs)
        else:
            raise ValueError('Must pass a CSS path or find attributes.')

        return attrs

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

        # Load API credentials
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
        
        Raises
        ------
        ValueError
            If "credentials" arg is not a str
            If provided pkl file is not valid
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
                        self.queue.put(
                            f'{token_name} not found. Attempting to run unverified...'
                        )
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

        Raises
        ------
        ValueError
            If no object paths are provided
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
                self.queue.put('Rate limit hit, waiting for request...')
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

        self.queue.put(f'Running {self.get_repo_name()}...')

        # Get search_output
        search_dict = self.get_all_search_outputs(**kwargs)

        # Set merge parameters
        merge_on = vars(self).get('merge_on')
        merge_right_on = vars(self).get('merge_right_on')
        merge_left_on = vars(self).get('merge_left_on')

        # Set save parameter
        save_dir = kwargs.get('save_dir')

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

        if save_dir:
            self.queue.put(f'Saving output to "{save_dir}".')
            self.save_dataframes(final_dict, save_dir)
            self.queue.put('Save complete.')
        
        self.queue.put(f'{self.get_repo_name()} run complete.')

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
            self.queue.put(f'Searching {search_term}.')
            search_dict[search_term] = self.get_individual_search_output(
                search_term, 
                flatten_output=flatten_output
            )
            self.queue.put('Search completed.')

        return search_dict

    @abstractmethod
    def get_individual_search_output(self, search_term, **kwargs):
        pass

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
            self.queue.put(f'Querying {query} metadata.')
            metadata_dict[query] = self.get_query_metadata(
                object_paths, 
                flatten_output=flatten_output
            )
            self.queue.put('Metadata query complete.')
        
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

        self.queue.put(f'Running {self.get_repo_name()}...')

        # Get search_output
        search_dict = self.get_all_search_outputs(**kwargs)

        # Set merge parameters
        merge_on = vars(self).get('merge_on')
        merge_right_on = vars(self).get('merge_right_on')
        merge_left_on = vars(self).get('merge_left_on')

        # Set save parameters
        save_dir = kwargs.get('save_dir')

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
            
        if save_dir:
            self.queue.put(f'Saving output to "{save_dir}".')
            self.save_dataframes(final_dict, save_dir)
            self.queue.put('Save complete.')
        
        self.queue.put(f'{self.get_repo_name()} run complete.')

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
            self.queue.put(f'Searching {search_term} {search_type}.')
            search_dict[(search_term, search_type)] = \
                self.get_individual_search_output(
                    search_term=search_term, 
                    search_type=search_type, 
                    flatten_output=flatten_output
                )
            self.queue.put('Search completed.')

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
            self.queue.put(f'Querying {search_term} {search_type} metadata.')

            metadata_dict[query] = self.get_query_metadata(
                object_paths=object_paths, 
                flatten_output=flatten_output
            )
            self.queue.put('Metadata query complete.')
        
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

        # Set save parameter
        save_dir = kwargs.get('save_dir')
        
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

        if save_dir:
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
            self.queue.put(f'Searching {search_type}.')
            search_dict[search_type] = self.get_individual_search_output(
                search_type,
                flatten_output=flatten_output
            )
            self.queue.put(f'{search_type} search completed.')

        return search_dict

    @abstractmethod
    def get_individual_search_output(self, search_type, **kwargs):
        pass

    def get_query_metadata(self, object_paths, search_type, **kwargs):
        # We raise an error instead of requiring implementation via
        # @abstractmethod since not all derived classes will require it
        raise NotImplementedError
