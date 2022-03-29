import re
from collections.abc import Collection
from time import sleep
from typing import Any, Optional, Union

import numpy as np
import pandas as pd
from selenium.webdriver.support.wait import WebDriverWait

from pycurator.collectors.base import (
    BaseCollector,
    BaseTermCollector,
    BaseWebCollector
)
from pycurator.utils import parse_numeric_string, web_utils
from pycurator.utils.parsing import validate_metadata_parameters
from pycurator.utils.typing import (
    SearchTerm,
    TermResultDict
)
from pycurator.utils.web_utils import text_to_be_present_on_page


class DryadCollector(BaseTermCollector, BaseWebCollector):
    """DataDryad collector for search term queries.

    This collector allows for both API collection and web scraping for
    additional attributes only available via the webpage for a given
    data record.

    Parameters
    ----------
    scrape : bool, optional (default=True)
        Flag for requesting web scraping as a method for additional
        metadata collection.
    search_terms : list-like, optional
        Terms to search over. Can be (re)set via set_search_terms()
        or passed in directly to search functions.
    credentials : str, optional (default=None)
        JSON filepath containing credentials in form
        {repository_name}: {key}.
    """

    def __init__(
        self,
        scrape: bool = True,
        search_terms: Optional[Collection[SearchTerm]] = None,
        credentials: Optional[bool] = None,
    ) -> None:
        self.scrape = scrape

        BaseTermCollector.__init__(
            self,
            repository_name='dryad',
            search_terms=search_terms,
            credentials=credentials
        )

        if self.scrape:
            BaseWebCollector.__init__(
                self,
                repository_name='dryad',
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
    def accepts_user_credentials() -> bool:
        return True

    @BaseCollector._pb_indeterminate
    def _conduct_search_over_pages(
        self,
        search_url: str,
        search_params: Any,
        print_progress: bool = False,
        delim: Optional[str] = None
    ) -> pd.DataFrame:
        """Query records from the Dryad API for given parameters.

        Parameters
        ----------
        search_url : str
        search_params : dict
            Contains parameters to pass to requests.get({params}).
            Most common include search term 'q', and page index 'page'.
            For full details, see the Notes.
        print_progress : bool, optional (default=False)
            If True, updates on query page progress is sent to object
            queue to be displayed in UI window.
        delim : bool, optional (default=None)
            Key to grab results from query response JSON. If None,
            entire JSON return is considered as the data results.

        Returns
        -------
        search_df : pandas.DataFrame

        Notes
        -----
        Dryad allows the following parameters when querying the noted
        record type.

        Datasets:
        page : int, optional
            Page to search over.
        per_page : int, optional
            Number of results per page.
        q : str, optional
            Term to query for.
        affiliation : str, optional
            ROR identifier to require in a dataset's authors.
        tenant : str, optional
            Tenant organization in Dryad. Ignored if affiliation given.
        modifiedSince : str, optional
            An ISO 8601 UTC timestamp for limiting results.

        Files of a dataset version:
        id : int
            Version ID of the dataset.
        page : int, optional
            As above.
        per_page : int, optional
            As above.

        When searching for file metadata, only id, as described above,
        is allowed.
        """

        search_df = pd.DataFrame()

        if print_progress:
            self._update_query_ref(
                search_term=search_params['q'],
                page=search_params['page']
            )
        _, output = self.get_request_output(search_url, params=search_params)

        while output.get('count'):
            output = output['_embedded']

            if delim:
                output = output[delim]

            output_df = pd.DataFrame(output)
            output_df['page'] = search_params['page']

            search_df = pd.concat([
                search_df, output_df]
            ).reset_index(drop=True)

            search_params['page'] += 1
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

    def get_individual_search_output(
            self,
            search_term: SearchTerm
    ) -> pd.DataFrame:
        """Returns information about all datasets from DataDryad.

        Parameters
        ----------
        search_term : str

        Returns
        -------
        search_df : pandas.DataFrame

        Raises
        ------
        TypeError
            Incorrect search_term type.
        """

        if not isinstance(search_term, str):
            raise TypeError(
                'search_term must be of type str, not'
                f' \'{type(search_term)}\'.'
            )

        search_url = f'{self.base_url}/search'
        search_params = {'q': search_term, 'page': 1, 'per_page': 100}

        search_df = self._conduct_search_over_pages(
            search_url=search_url,
            search_params=search_params,
            print_progress=True,
            delim='stash:datasets'
        )

        # Add dataset-specific version id for metadata querying
        search_df['version'] = self._extract_version_ids(search_df)

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

    def get_query_metadata(
            self,
            object_paths: Union[str, Collection[str]],
            **kwargs: Any
    ) -> pd.DataFrame:
        """Retrieves the metadata for the object_paths objects.

        Parameters
        ----------
        object_paths : str or collection of str

        Returns
        -------
        metadata_df : pandas.DataFrame

        Raises
        ------
        TypeError
            If no object paths are provided.
        """

        object_paths = validate_metadata_parameters(object_paths)

        start_page = 1
        metadata_df = pd.DataFrame()

        for object_path in self._pb_determinate(object_paths):
            search_url = f'{self.base_url}/versions/{object_path}/files'
            search_params = {'page': start_page}

            object_df = self._conduct_search_over_pages(
                search_url,
                search_params,
                delim='stash:files',
                print_progress=False
            )

            object_df['version'] = object_path
            object_df.loc[:, 'page'] = search_params['page']
            metadata_df = pd.concat(
                [metadata_df, object_df]
            ).reset_index(drop=True)

        return metadata_df

    def get_web_output(self, object_urls: Collection[str]) -> pd.DataFrame:
        """Scrapes path_dict attributes for the provided object urls.

        Parameters
        ----------
        object_urls : collection of str

        Returns
        -------
        search_df : pandas.DataFrame
        """

        self.status_queue.put('Scraping web metadata...')

        search_df = pd.DataFrame()

        for url in self._pb_determinate(object_urls):
            self.driver.get(url)
            soup = self._get_soup(features='html.parser')

            while 'Request rejected due to rate limits.' in soup.text:
                self.status_queue.put(
                    'Rate limit hit, pausing for one minute...'
                )
                sleep(60)
                self.driver.get(url)
                soup = self._get_soup(features='html.parser')

            WebDriverWait(self.driver, 10).until(
                text_to_be_present_on_page(self.attr_dict['numViews'])
            )
            soup = self._get_soup(features='html.parser')

            object_dict = {
                var: web_utils.get_tag_value(
                    web_utils.get_single_tag(
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
            object_dict['title'] = web_utils.get_tag_value(
                web_utils.get_single_tag(
                    soup=soup,
                    path='h1.o-heading__level1'
                )
            )

            search_df = pd.concat(
                [search_df, pd.DataFrame([object_dict])]
            ).reset_index(drop=True)

        self.status_queue.put('Web metadata scraping complete.')

        return search_df

    def _extract_version_ids(self, df: pd.DataFrame) -> pd.DataFrame:
        """Retrieve ids from DataFrame entries."""
        return df['_links'].apply(
            lambda entry: entry.get('stash:version', {})
                               .get('href', '')
                               .split('/')[-1]
            if entry is not np.nan else None
        )

    def get_all_metadata(self, search_dict: TermResultDict) -> TermResultDict:
        """Retrieves metadata for records contained in input DataFrames.

        Parameters
        ----------
        search_dict : dict
            Dictionary of DataFrames from get_all_search_outputs.

        Returns
        -------
        metadata_dict : dict
        """

        object_path_dict = {
            query: self._extract_version_ids(df)
            for query, df in search_dict.items()
        }

        metadata_dict = super().get_all_metadata(
            object_path_dict=object_path_dict
        )

        return metadata_dict
