import re
from time import sleep

import pandas as pd
from flatten_json import flatten
from tqdm import tqdm

from scrapers.base_scrapers import AbstractScraper, AbstractTermScraper, \
    AbstractWebScraper
from utils import parse_numeric_string


class DryadScraper(AbstractTermScraper, AbstractWebScraper):
    """Scrapes the Dryad API for all data relating to the given search terms.

    Parameters
    ----------
    path_file : str, optional (default=None)
        Json file for loading path dict.
        Must be of the form {search_type: {path_type: path_dict}}
    scrape : boolearn, optional (default=True)
        Flag for requesting web scraping as a method for additional metadata
        collection. 
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

    Notes
    -----
    The method of webscraping has changed for this class, rendering the presence
    of a path_file unnecessary. The class initialization will change at a later
    point to reflect this.
    """

    def __init__(
        self, 
        path_file=None,
        scrape=True,
        search_terms=None, 
        flatten_output=None, 
        credentials=None,
    ):
        self.scrape = scrape

        AbstractTermScraper.__init__(
            self,
            repository_name='dryad', 
            search_terms=search_terms, 
            credentials=credentials,
            flatten_output=flatten_output
        )

        if self.scrape:
            AbstractWebScraper.__init__(
                self,
                repository_name='dryad',
                path_file=path_file,
            )

            self.scrape_url = 'https://datadryad.org/stash/dataset'

            self.attr_dict = {
                'numViews': r'\d+ views',
                'numDownloads': r'\d+ downloads',
                'numCitations': r'\d+ citations'
            }

        self.base_url = 'https://datadryad.org/api/v2'
        self.merge_on = 'version'

    @staticmethod
    def accept_user_credentials():
        return False

    @AbstractScraper._pb_indeterminate
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
            self._update_query_ref(
                search_term=search_params['q'],
                page=search_params['page']
            )
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

            search_df = pd.concat([
                search_df, output_df]
            ).reset_index(drop=True)

            # Increment page to search over
            search_params['page'] += 1

            # Perform next search and convert results to json
            if print_progress:
                self._update_query_ref(
                    search_term=search_params['q'],
                    page=search_params['page']
                )   
            _, output = self.get_request_output(
                url=search_url, 
                params=search_params
            )

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
        search_df : pandas.DataFrame
        """

        flatten_output = kwargs.get('flatten_output', self.flatten_output)

        # Set search params
        search_url = f'{self.base_url}/search'

        search_params = {'q': search_term, 'page': 1, 'per_page': 100}
        
        search_df = self._conduct_search_over_pages(
            search_url=search_url, 
            search_params=search_params, 
            flatten_output=flatten_output, 
            print_progress=True,
            delim='stash:datasets'
        )

        # Add dataset-specific version id for metadata querying
        search_df['version'] = self._extract_version_ids(search_df)

        # Scrape additional metadata from object webpages
        if self.scrape:
            urls = search_df['identifier'].apply(
                lambda doi: f'{self.scrape_url}/{doi}'
            )
            web_df = self.get_web_output(urls)
            
            search_df = pd.merge(
                search_df, 
                web_df, 
                how='outer', 
                on='identifier'
            )

        return search_df

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
        for object_path in self._pb_determinate(object_paths):
            search_url = f'{self.base_url}/versions/{object_path}/files'
            search_params = {'page': start_page}

            # Conduct search
            object_df = self._conduct_search_over_pages(
                search_url, 
                search_params, 
                flatten_output, 
                delim='stash:files',
                print_progress=False
            )

            # Add relevant data to DataFrame and merge
            object_df['version'] = object_path
            object_df['page'] = search_params['page']
            metadata_df = pd.concat(
                [metadata_df, object_df]
            ).reset_index(drop=True)

        return metadata_df

    def get_web_output(self, object_urls, **kwargs):
        """Scrapes the attributes in path_dict for the provided object urls.

        Parameters
        ----------
        object_urls

        Returns
        -------
        search_df : pandas.DataFrame
        """

        self.queue.put('Scraping web metadata...')

        search_df = pd.DataFrame()

        for url in self._pb_determinate(object_urls):
            self.driver.get(url)
            soup = self._get_soup(features='html.parser')

            while (
                'Request rejected due to rate limits.' in soup.text or 
                not self._get_attribute_value(
                        self.get_single_attribute(
                            soup=soup, 
                            string=re.compile(self.attr_dict['numViews'])
                        )
                    )
            ):
                self.queue.put('Rate limit hit, waiting for request...')
                sleep(5)
                self.driver.get(url)
                soup = self._get_soup(features='html.parser')

            object_dict = {
                var: self._get_attribute_value(
                    self.get_single_attribute(
                        soup=soup, 
                        string=re.compile(attr)
                    )
                )
                for var, attr in self.attr_dict.items()
            }

            # Clean results
            for key, value in object_dict.items():
                object_dict[key] = parse_numeric_string(value)

            # Add identifier for merging
            object_dict['identifier'] = '/'.join(url.split('/')[-2:])
            object_dict['title'] = self._get_attribute_value(
                self.get_single_attribute(
                    soup=soup, 
                    path='h1.o-heading__level1'
                )
            )

            search_df = search_df.append(object_dict, ignore_index=True)
        
        self.queue.put('Web metadata scraping complete.')

        return search_df

    def _extract_version_ids(self, df):
        if self.flatten_output:
            return df['_links_stash:versions_href'].apply(
                lambda row: row.split('/')[-1]
            )
        else:
            return df['_links'].apply(
                lambda row: row['stash:version']['href'].split('/')[-1]
            )

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

        object_path_dict = {
            query: self._extract_version_ids(df)
            for query, df in search_dict.items()
        }
       
        metadata_dict = super().get_all_metadata(
            object_path_dict=object_path_dict,
            **kwargs
        )

        return metadata_dict
