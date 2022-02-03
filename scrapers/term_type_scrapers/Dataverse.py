import os
import re
from collections import OrderedDict

import pandas as pd
from flatten_json import flatten
from tqdm import tqdm

from scrapers.base_scrapers import AbstractScraper, AbstractTermTypeScraper, \
    AbstractWebScraper
from utils import parse_numeric_string


class DataverseScraper(AbstractTermTypeScraper, AbstractWebScraper):
    """Scrapes Dataverse API for all data relating to the given search params.

    Parameters
    ----------
    path_file : str, optional 
            (default=os.path.join('paths', 'dataverse_paths.json'))
        Json file for loading path dict.
        Must be of the form {search_type: {path_type: path_dict}}
    scrape : boolearn, optional (default=True)
        Flag for requesting web scraping as a method for additional metadata
        collection. 
    search_terms : list-like, optional (default=None)
        Terms to search over. Can be (re)set via set_search_terms() or passed in
        directly to search functions.
    search_types : list-like, optional (default=None)
        Data types to search over. Can be (re)set via set_search_types() or 
        passed in directly to search functions to override set parameter.
    flatten_output : boolean, optional (default=None)
        Flag for specifying if nested output should be flattened. Can be passed
        in directly to functions to override set parameter.
    credentials : str, optional (default=None)
        JSON filepath containing credentials in form {repository_name}: 'key'.
    """

    def __init__(
        self,
        path_file=os.path.join('paths', 'dataverse_paths.json'),
        scrape=True,
        search_terms=None,
        search_types=None,
        flatten_output=None,
        credentials=None,
    ):

        self.scrape = scrape

        AbstractTermTypeScraper.__init__(
            self,
            repository_name='dataverse', 
            search_terms=search_terms,
            search_types=search_types, 
            flatten_output=flatten_output
        )

        if self.scrape:
            AbstractWebScraper.__init__(
                self,
                repository_name='dataverse',
                path_file=path_file,
            )

        self.base_url = 'https://dataverse.harvard.edu/api'
        self.file_url = 'https://dataverse.harvard.edu/file.xhtml?fileId='
        self.data_url = 'https://dataverse.harvard.edu/dataset.xhtml?persistentId='
        self.headers = dict()

        if credentials:
            self.load_credentials(credential_filepath=credentials)

    @staticmethod
    def accept_user_credentials():
        return True

    @classmethod
    def get_search_type_options(cls):
        return ('dataset', 'file')

    def load_credentials(self, credential_filepath):
        """Load the credentials given filepath or token.

        Parameters
        ----------
        credential_filepath : str, 
            JSON filepath containing credentials in form 
            {repository_name}: 'key'.
        """

        super().load_credentials(credential_filepath)
        self.headers['X-Dataverse-key'] = self.credentials
    
    @AbstractScraper._pb_indeterminate
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
        search_type_options = DataverseScraper.get_search_type_options()

        # Validate input
        if not isinstance(search_term, str):
            raise ValueError('Search term must be a string.')
        if search_type not in search_type_options:
            raise ValueError(f'Can only search {search_type_options}.')

        # Set search parameters
        start = 0
        page_size = 25
        search_df = pd.DataFrame()
        page_idx = 0

        search_params = {
            'q': search_term,
            'per_page': page_size,
            'start': start,
            'type': search_type
        }

        # Conduct initial search & extract results
        _, output = self.get_request_output_and_update_query_ref(
            url=search_url,
            params=search_params,
            headers=self.headers,
            search_term=search_term,
            search_type=search_type,
            page=page_idx
        )
        output = output['data']

        # Search until no more items are returned
        while output.get('items'):
            output = output['items']

            # Flatten results if necessary
            if flatten_output:
                output = [flatten(result) for result in output]
            
            output_df = pd.DataFrame(output)
            output_df['page'] = (
                search_params['start'] // search_params['per_page'] + 1
            )

            search_df = pd.concat([search_df, output_df]).reset_index(drop=True)

            # Increment result offset to perform another search
            search_params['start'] += search_params['per_page']
            page_idx += 1

            # Perform next search and convert results to json
            _, output = self.get_request_output_and_update_query_ref(
                url=search_url,
                params=search_params,
                headers=self.headers,
                search_term=search_term,
                search_type=search_type,
                page=page_idx
            )
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


    def _scrape_file_info(self):
        file_info_list = []
        soup = self._get_soup(features='html.parser')
        try:
            file_info = soup.find(
                'script', 
                {'type': r'application/ld+json'}
            ).contents[0]

            # Get the file info & remove unnecessary delimiters
            file_info = file_info.split(r'"distribution":')[1]
            file_info = file_info.replace('},{', '}{').strip('[]').split('}{')
        except:
            return file_info_list
        
        # Remove unnecessary attributes
        file_info = [
            re.sub(r'"@type":"(.*?)",', '', entry).strip('{}').replace('"', '') 
            for entry in file_info
        ]
        
        # Transform each string into dict
        file_info = [entry.split(',', maxsplit=1) for entry in file_info]
        
        for file_list in file_info:
            file_dict = {}
            for attr_str in file_list:
                try:
                    key, val = attr_str.split(':', maxsplit=1)
                    file_dict[key] = val
                except ValueError as e:
                    print(file_list)
                    raise e
                
            # Convert file size from str to int
            if file_dict.get('contentSize'):
                file_dict['contentSize'] = int(file_dict['contentSize'])
                
            file_info_list.append(file_dict)
        
        return file_info_list

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
                attribute: self._get_attribute_value(
                    self.get_single_attribute(soup=soup, path=path)
                )
                    for attribute, path in landing_attribute_paths.items()
            }
            attribute_value_dict = {
                **attribute_value_dict, 
                **landing_attribute_values
            }
        if metadata_attribute_paths:
            self.driver.refresh()
            self.driver.find_element_by_link_text('Metadata').click()
            
            # Retrieve html data and create parsable object
            soup = self._get_soup(features='html.parser')
            
            metadata_attribute_values = {
                attribute: self._get_attribute_value(
                    self.get_single_attribute(soup=soup, path=path) 
                )
                    for attribute, path in metadata_attribute_paths.items()
            }
            attribute_value_dict = {
                **attribute_value_dict, 
                **metadata_attribute_values
            }

        if terms_attribute_paths:
            self.driver.refresh()
            self.driver.find_element_by_link_text('Terms').click()

            # Retrieve html data and create parsable object
            soup = self._get_soup(features='html.parser')
            
            terms_attribute_values = {
                attribute: self._get_attribute_value(
                    self.get_single_attribute(soup=soup, path=path) 
                )
                    for attribute, path in terms_attribute_paths.items()
            }
            attribute_value_dict = {
                **attribute_value_dict, 
                **terms_attribute_values
            }
            
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
            results['num_downloads'] = parse_numeric_string(num_downloads)
            
        return results   

    def get_query_metadata(self, object_paths, **attribute_dicts):
        """
        Retrieves the metadata for the object/objects listed in object_paths.
        
        Parameters
        ----------
        object_paths : str/list-like
            String or list of strings containing the paths for the objects.
        attribute_dicts : dict, optional
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
        for object_path in self._pb_determinate(object_paths):
            object_dict = dict()

            # Retrieve webpage
            self.driver.get(object_path)

            # Extract & clean attribute values
            try:
                object_dict = self._get_attribute_values(**attribute_dicts)
            except:
                print(object_path)
                raise ValueError
            
            # Don't need to try scraping files for file objects
            if 'dataset' in object_path:
                object_dict['files'] = self._scrape_file_info()

            object_dict = self._clean_results(object_dict)

            # Add results to DataFrame
            metadata_df = metadata_df.append(object_dict, ignore_index=True)

        # Remove any objects that did not return metadata (fully null rows)
        metadata_df = metadata_df.dropna(how='all')

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

        if not self.scrape:
            return metadata_dict

        for query, df in search_dict.items():
            if df is not None:
                search_term, search_type = query
                self.queue.put(
                    f'Querying {search_term} {search_type} metadata.'
                )

                if search_type == 'dataset':
                    object_paths = df['global_id'].apply(
                        lambda doi: f'{self.data_url}{doi}'
                    )
                elif search_type == 'file':
                    object_paths = df['url']
            
                metadata_dict[query] = self.get_query_metadata(
                    object_paths, 
                    **self.path_dict[search_type]
                )
            
            self.queue.put(
                f'{search_term} {search_type} metadata query complete.'
            )

        return metadata_dict
