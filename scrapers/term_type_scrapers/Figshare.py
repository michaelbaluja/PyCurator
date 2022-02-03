from collections import OrderedDict

import pandas as pd
from flatten_json import flatten
from tqdm import tqdm

from scrapers.base_scrapers import AbstractScraper, AbstractTermTypeScraper


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
    credentials : str, optional (default=None)
        JSON filepath containing credentials in form {repository_name}: 'key'.
    """

    search_type_options = ('articles', 'collections', 'projects')

    def __init__(
        self,
        search_terms=None,
        search_types=None,
        flatten_output=None,
        credentials=None
    ):
        super().__init__(
            repository_name='figshare', 
            search_terms=search_terms,
            search_types=search_types,
            flatten_output=flatten_output, 
            credentials=None
        )
        self.base_url = 'https://api.figshare.com/v2'
        self.merge_on = 'id'
        self.headers = dict()

        if credentials:
            self.load_credentials(credential_filepath=credentials)

    @staticmethod
    def accept_user_credentials():
        return True

    def load_credentials(self, credential_filepath):
        """Load the credentials given filepath or token.

        Parameters
        ----------
        credential_filepath : str, optional (default=credentials.pkl)
            JSON filepath containing credentials in form 
            {repository_name}: 'key'.
        """

        super().load_credentials(credential_filepath)
        self.headers['Authorization'] = f'token {self.credentials}'

    @AbstractScraper._pb_indeterminate
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
        response, output = self.get_request_output_and_update_query_ref(
            url=search_url,
            params=search_params,
            headers=self.headers,
            published_since=search_date,
            page=start_page
        )

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
                search_df = pd.concat(
                    [search_df, output_df]
                ).reset_index(drop=True)

                # Increment page number to query over
                search_params['page'] += 1

                # Conduct search
                response, output = self.get_request_output_and_update_query_ref(
                    url=search_url,
                    params=search_params,
                    headers=self.headers,
                    published_since=search_params['published_since'],
                    page=search_params['page']
                )
            try:
                # If we did not get a full page of results, search is complete
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
            response, output = self.get_request_output(
                url=search_url, 
                params=search_params, 
                headers=self.headers
            )
            response, output = self.get_request_output_and_update_query_ref(
                url=search_url,
                params=search_params,
                headers=self.headers,
                published_since=search_params['published_since'],
                page=search_params['page']
            )

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
        for object_path in self._pb_determinate(object_paths):
            # Download the metadata
            _, json_data = self.get_request_output(
                url=object_path, 
                headers=self.headers
            )

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
                object_paths = [
                    f'{self.base_url}/{search_type}/{object_id}' 
                    for object_id in object_ids
                ]
                
                object_path_dict[query] = object_paths
        
        metadata_dict = super().get_all_metadata(
            object_path_dict, 
            flatten_output=flatten_output
        )

        return metadata_dict
