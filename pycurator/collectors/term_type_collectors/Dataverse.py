import ast
import os
from collections.abc import Collection
from typing import Optional, Union

import pandas as pd

from pycurator._typing import (
    AttributeDict,
    SearchTerm,
    SearchType,
    TermTypeResultDict
)
from pycurator.collectors import (
    BaseCollector,
    BaseTermTypeCollector,
    BaseWebCollector,
    WebPathScraperMixin
)
from pycurator.utils.parsing import (
    parse_numeric_string,
    validate_metadata_parameters
)
from pycurator.utils.web_utils import (
    get_single_tag,
    get_single_tag_from_tag_info,
    get_tag_value
)


class DataverseCollector(
    BaseTermTypeCollector,
    BaseWebCollector,
    WebPathScraperMixin
):
    """Harvard Dataverse collector for search term and type queries.

    This collector allows for both API collection and web scraping for
    additional attributes only available via the webpage for a given
    dataset or file.

    Parameters
    ----------
    path_file : str, optional
            (default=os.path.join('paths', 'dataverse_paths.json'))
        JSON file for loading path dict.
        Must be of the form {search_type: {path_type: path_dict}}
    scrape : bool, optional (default=True)
        Flag for requesting web scraping as a method for additional
        metadata collection.
    search_terms : list-like, optional (default=None)
        Terms to search over. Can be (re)set via set_search_terms()
        or passed in directly to search functions.
    search_types : list-like, optional (default=None)
        Data types to search over. Can be (re)set via set_search_types()
        or passed in directly to search functions to override set
        parameter.
    credentials : str, optional (default=None)
        JSON filepath containing credentials in form
        {repository_name}: {key}.
    """

    def __init__(
        self,
        path_file: str = os.path.join('paths', 'dataverse_paths.json'),
        scrape: bool = True,
        search_terms: Optional[Collection[SearchTerm]] = None,
        search_types: Optional[Collection[SearchType]] = None,
        credentials: Optional[str] = None,
    ) -> None:

        self.scrape = scrape

        BaseTermTypeCollector.__init__(
            self,
            repository_name='dataverse',
            search_terms=search_terms,
            search_types=search_types
        )

        if self.scrape:
            self.path_dict = path_file
            BaseWebCollector.__init__(
                self,
                repository_name='dataverse'
            )

        base_url = 'https://dataverse.harvard.edu'
        self.api_url = f'{base_url}/api'
        self.file_url = f'{base_url}/file.xhtml?fileId='
        self.data_url = f'{base_url}/dataset.xhtml?persistentId='
        self.headers = dict()

        if credentials:
            self.credentials = self.load_credentials(
                credential_filepath=credentials
            )

    @staticmethod
    def accepts_user_credentials() -> bool:
        return True

    @classmethod
    @property
    def search_type_options(cls) -> tuple[SearchType, ...]:
        return ('dataset', 'file')

    def load_credentials(self, credential_filepath: str) -> Union[str, None]:
        """Load the credentials given filepath or token.

        Parameters
        ----------
        credential_filepath : str,
            JSON filepath containing credentials in form
            {repository_name}: {key}.

        Returns
        -------
        credentials : str or None
        """

        credentials = super().load_credentials(
            credential_filepath=credential_filepath
        )
        self.headers['X-Dataverse-key'] = credentials
        return credentials

    @BaseCollector._pb_indeterminate
    def get_individual_search_output(
            self,
            search_term: SearchTerm,
            search_type: SearchType
    ) -> pd.DataFrame:
        """Queries Dataverse API for the specified search term and type.

        Parameters
        ----------
        search_term : str
        search_type : {'dataset', 'file'}

        Returns
        -------
        search_df : pandas.DataFrame

        Raises
        ------
        TypeError
            Incorrect search_term type.
        ValueError
            Invalid search_type provided.
        """

        search_url = f'{self.api_url}/search'
        search_type_options = self.search_type_options

        if not isinstance(search_term, str):
            raise TypeError(
                'search_term must be of type str, not'
                f' \'{type(search_term)}\'.'
            )
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

        while output.get('items'):
            output = output['items']

            output_df = pd.DataFrame(output)
            output_df['page'] = (
                search_params['start'] // search_params['per_page'] + 1
            )

            search_df = pd.concat(
                [search_df, output_df]
            ).reset_index(drop=True)

            search_params['start'] += search_params['per_page']
            page_idx += 1

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
                search_df.loc[:, 'url'] = search_df.apply(
                    lambda row: f'{self.file_url}{row.file_id}',
                    axis=1
                )

        return search_df

    def _scrape_file_info(self) -> list[Union[AttributeDict, None]]:
        """Scrapes the file info for the current dataset.

        For a page in self.driver, scrapes any information available
        about the listed files.

        Returns
        -------
        list of dicts of {'file_attr': val} or empty
        """

        soup = self._get_soup(features='html.parser')
        header_metadata_tag = get_single_tag_from_tag_info(
            soup=soup,
            class_type='script',
            attrs={'type': r'application/ld+json'}
        )

        header_metadata_dict = ast.literal_eval(
            header_metadata_tag.text.strip()
        )

        return header_metadata_dict.get('distribution', [])

    def _get_attribute_values(
            self,
            **kwargs: Optional[Union[AttributeDict, Collection[AttributeDict]]]
    ) -> AttributeDict:
        """Return values for all relevant attribute path dicts provided.

        Parameters
        ----------
        **kwargs : dict, optional
            Attribute dicts to parse through. Utilizes landing page,
            metadata, and terms dicts, when provided.

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
            soup = self._get_soup(features='html.parser')

            landing_attribute_values = {
                attribute: get_tag_value(
                    get_single_tag(soup=soup, path=path)
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
            soup = self._get_soup(features='html.parser')

            metadata_attribute_values = {
                attribute: get_tag_value(
                    get_single_tag(soup=soup, path=path)
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
            soup = self._get_soup(features='html.parser')

            terms_attribute_values = {
                attribute: get_tag_value(
                    tag=get_single_tag(soup=soup, path=path)
                )
                for attribute, path in terms_attribute_paths.items()
            }
            attribute_value_dict = {
                **attribute_value_dict,
                **terms_attribute_values
            }

        return attribute_value_dict

    def _clean_results(self, results: AttributeDict) -> AttributeDict:
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
            results['num_downloads'] = parse_numeric_string(
                num_downloads
            )

        return results

    def get_query_metadata(
            self,
            object_paths: Collection[str],
            **attribute_dicts: dict[str, AttributeDict]
    ) -> pd.DataFrame:
        """Retrieves metadata for the object_paths objects.

        Parameters
        ----------
        object_paths : str or list-like
            String or list of strings containing object paths.
        **attribute_dicts : dict, optional
            Holds attribute paths for scraping metadata.

        Returns
        -------
        metadata_df : pandas.DataFrame
            Metadata for the requested objects.
        """

        object_paths = validate_metadata_parameters(object_paths)

        metadata_df = pd.DataFrame()

        for object_path in self._pb_determinate(object_paths):
            self.driver.get(object_path)

            object_dict = self._get_attribute_values(**attribute_dicts)

            if 'dataset' in object_path:
                object_dict['files'] = self._scrape_file_info()

            object_dict = self._clean_results(object_dict)

            metadata_df = pd.concat(
                [metadata_df, pd.DataFrame([object_dict])]
            ).reset_index(drop=True)

        # Remove any objects that did not return metadata (fully null rows)
        metadata_df = metadata_df.dropna(how='all')

        return metadata_df

    def get_all_metadata(
            self,
            search_dict: TermTypeResultDict
    ) -> TermTypeResultDict:
        """Retrieves metadata for records contained in input DataFrames.

        Parameters
        ----------
        search_dict : dict
            Dictionary of DataFrames from get_all_search_outputs.

        Returns
        -------
        metadata_dict : dict
            Dictionary of DataFrames with metadata for each query.

        Raises
        ------
        ValueError
            Invalid search_type in query.
        """

        metadata_dict = dict()

        if not self.scrape:
            return metadata_dict

        for query, df in search_dict.items():
            if df is not None and not df.empty:
                search_term, search_type = query
                self.status_queue.put(
                    f'Querying {search_term} {search_type} metadata.'
                )

                if search_type == 'dataset':
                    object_paths = df['global_id'].apply(
                        lambda doi: f'{self.data_url}{doi}'
                    )
                elif search_type == 'file':
                    object_paths = df['url']
                else:
                    raise ValueError(
                        f'Can only search {self.search_type_options}.'
                    )

                metadata_dict[query] = self.get_query_metadata(
                    object_paths=object_paths,
                    **self.path_dict[search_type]
                )

                self.status_queue.put(
                    f'{search_term} {search_type} metadata query complete.'
                )

        return metadata_dict
