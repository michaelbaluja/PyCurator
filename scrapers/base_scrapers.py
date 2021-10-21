import json
import os
import pickle
import time
from abc import ABC, abstractmethod, abstractstaticmethod
from collections import OrderedDict

import pandas as pd
import requests
from bs4 import BeautifulSoup


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
                output_filename = f'{query}.csv'
            else:
                search_term, search_type = query
                output_filename = f'{search_term}_{search_type}.csv'
            
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
            results.to_csv(filepath)
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

    def __init__(self, repository_name, driver, path_file, flatten_output=False):
        AbstractScraper.__init__(
            self,
            repository_name=repository_name, 
            flatten_output=flatten_output
        )
        self.driver = driver

        with open(path_file) as f:
            self.path_dict = json.load(f)

    def _get_soup(self, **kwargs):
        html = self.driver.page_source
        return BeautifulSoup(html, **kwargs)

    def get_single_attribute_value(self, soup, path):
        """Retrieves the requested value from the soup object.
        
        For a page attribute with a single value 
        ('abstract', 'num_instances', etc), returns the value. 
        For attributes with potentially multiple values, such as 'keywords', 
        use get_variable_attribute_values(...)
        
        Parameters
        ----------
        soup : BeautifulSoup
            BeautifulSoup object containing the html to be parsed.
        path : str
            CSS Selector Path for attribute to scrape.
        
        Returns
        -------
        value of attribute
        """

        try:
            return soup.select_one(path).text
        except AttributeError as e:
            return None

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

        return [tag.text for tag in soup.select(path)]


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
                print('Rate limiting\n\n\n')
                print(vars(r))
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
                    search_df, 
                    metadata_df, 
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
