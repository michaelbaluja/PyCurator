import warnings

import pandas as pd
from flatten_json import flatten

from scrapers.base_scrapers import AbstractScraper, AbstractTermScraper


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
    credentials : str, optional (default=None)
        JSON filepath containing credentials in form {repository_name}: 'key'.
    """

    def __init__(self, search_terms=None, flatten_output=None,
                 credentials=None):
        super().__init__('zenodo', search_terms, flatten_output, credentials)
        self.base_url = 'https://zenodo.org/api/records'

    @staticmethod
    def accept_user_credentials():
        return True

    @AbstractScraper._pb_indeterminate
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

        search_params = {
            'q': f'{search_term} AND created:[{start_date} TO {end_date}]',
            'page': 1,
            'size': 1000
        }

        # Run initial search & extract output
        response, output = self.get_request_output_and_update_query_ref(
            url=self.base_url,
            params=search_params,
            search_term=search_term,
            year=search_year,
            page=search_params['page']
        )

        # Handle any potential errors
        if response.status_code != 200:
            warnings.warn(f'{response.status_code}: Returning without results.')
            self.continue_running = False
            self.terminate()

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
                    [search_df, output_df]
                ).reset_index(drop=True)

                # Increment page for next search
                search_params['page'] += 1
                
                # Run search & extract output
                response, output = self.get_request_output_and_update_query_ref(
                    url=self.base_url,
                    params=search_params,
                    search_term=search_term,
                    year=search_year,
                    page=search_params['page']
                )

            # Change search year, reset search page
            search_year -= 1
            start_date = f'{search_year}-01-01'
            end_date = f'{search_year}-12-31'

            search_params['q'] = \
                f'{search_term} AND created:[{start_date} TO {end_date}]'
            search_params['page'] = 1

            # Run search & extract output
            response, output = self.get_request_output_and_update_query_ref(
                url=self.base_url,
                params=search_params,
                search_term=search_term,
                year=search_year,
                page=search_params['page']
            )

        self.num_queries = False

        return search_df
