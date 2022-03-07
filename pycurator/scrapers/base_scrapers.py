import itertools
import json
import os
import queue
import sys
import time
from abc import ABC, abstractmethod
from collections.abc import (
    Iterable,
    Collection,
    Callable,
    Generator,
    Hashable,
    Sequence
)
from typing import Any, AnyStr, NoReturn, Optional, ParamSpec, TypeVar, Union

import pandas as pd
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

import pycurator.utils
from pycurator.utils.typing import (
    JSONDict,
    SearchTerm,
    SearchType,
    SearchTuple,
    TermResultDict,
    TermTypeResultDict,
    QueryResultDict
)

T = TypeVar('T')
P = ParamSpec('P')


class AbstractScraper(ABC):
    """
    Contains basic functions that are relevant for all scraper objects.

    Parameters
    ----------
    repository_name : str
        Name of the repository being scraped. Used for loading credentials and
        saving output results.
    flatten_output : bool, optional (default=False)
        Flag for specifying if nested output should be flattened. Can be passed
        in directly to functions to override set parameter.
    """

    def __init__(
            self,
            repository_name: str,
            flatten_output: bool = False
    ) -> None:
        self.repository_name = repository_name
        self.flatten_output = flatten_output
        self.continue_running = True

        # Container for holding status update messages
        self.queue = queue.Queue()

        # Variable for updating progress over structured loop queries
        self.num_queries = None
        self.queries_completed = None
        self.current_query_ref = None

    def _pb_determinate(self, coll: Collection[T]) -> Generator[T, None, None]:
        """Generator for iterating data and updating progress bar status.

        Parameters
        ----------
        coll : iterable

        Yields
        ------
        next object of coll
        """

        assert hasattr(coll, '__iter__'), 'Parameter "coll" must be iterable.'

        # Initialize tracking vars
        if not self.num_queries:
            self.num_queries = len(coll)
            self.queries_completed = 0

        # Yield next item in coll and update tracking vars
        for item in coll:
            self.current_query_ref = str(item)
            yield item
            self.queries_completed += 1

        # After all items have been yielded, reset tracking vars
        self.num_queries = None
        self.queries_completed = None
        self.current_query_ref = None

    @abstractmethod
    def run(self) -> NoReturn:
        raise NotImplementedError(
            'Subclass must implement "run()".'
        )

    @staticmethod
    def _pb_indeterminate(
            indeterminate_query_func: Callable[P, T]
    ) -> Callable[P, T]:
        """Progress bar wrapper for indeterminate-length queries."""

        def update_pb(
                self,
                *args: P.args,
                **kwargs: P.kwargs
        ) -> Iterable[Any]:
            self.num_queries = True
            results = indeterminate_query_func(self, *args, **kwargs)
            self.num_queries = False
            return results

        return update_pb

    def request_execution(self) -> None:
        """Raise flag to stop output."""
        self.continue_running = False

    def terminate(self) -> NoReturn:
        """Handle program execution."""
        self.queue.put('Requesting program termination.')
        sys.exit()

    @staticmethod
    @abstractmethod
    def accepts_user_credentials() -> NoReturn:
        raise NotImplementedError(
            'Subclass must implement "accepts_user_credentials()".'
        )

    def _print_progress(self, page: str) -> None:
        """Update queue with current page being searched."""
        self.queue.put(f'Searching page {page}')

    def _update_query_ref(self, **kwargs: Any) -> None:
        """Combine keywords and update self.current_query_ref."""
        self.current_query_ref = kwargs


class WebPathScraperMixin:
    @property
    def path_dict(self) -> JSONDict:
        return self._path_dict

    @path_dict.setter
    def path_dict(self, path_file: str) -> None:
        if not os.path.exists(path_file):
            raise FileNotFoundError(
                f'Path file \'{path_file}\' does not exist.'
            )
        with open(path_file) as f:
            self._path_dict = json.load(f)


class AbstractWebScraper(AbstractScraper):
    """Base class for all repository web scrapers.

    Contains basic functions that are relevant for all derived classes.

    Parameters
    ----------
    repository_name : str
        Name of the repository being scraped. Used for loading credentials and
        saving output results.
        Web scrapers do not require user credentials at all.
    flatten_output : bool, optional (default=False)
        Flag for specifying if nested output should be flattened. Can be passed
        in directly to functions to override set parameter.
    """

    def __init__(
            self,
            repository_name: str,
            flatten_output: bool = False
    ) -> None:
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

    @staticmethod
    def accepts_user_credentials() -> bool:
        return False

    @abstractmethod
    def run(self) -> NoReturn:
        raise NotImplementedError(
            'Subclass must implement "run()".'
        )

    def _get_soup(self, **kwargs: Any) -> BeautifulSoup:
        """Return a BeautifulSoup object for the object driver current page."""
        # If user has requested termination, handle cleanup instead of querying
        # additional results
        if not self.continue_running:
            self.terminate()

        html = self.driver.page_source
        return BeautifulSoup(html, **kwargs)


class AbstractAPIScraper(AbstractScraper):
    """Base class for all repository API scrapers.

    Contains basic functions that may be necessary for API scrapers.

    Parameters
    ----------
    repository_name : str
        Name of the repository being scraped. Used for loading credentials and
        saving output results.
        Web scrapers do not require user credentials at all.
    flatten_output : bool, optional (default=False)
        Flag for specifying if nested output should be flattened. Can be passed
        in directly to functions to override set parameter.
    credentials : str, optional (default=None)
        JSON filepath containing credentials in form {repository_name}: 'key'.
    """

    def __init__(
            self,
            repository_name: str,
            flatten_output: bool = False,
            credentials: Optional[str] = None
    ) -> None:
        AbstractScraper.__init__(
            self,
            repository_name=repository_name,
            flatten_output=flatten_output
        )

        # Load API credentials
        if credentials:
            self.credentials = self.load_credentials(
                credential_filepath=credentials
            )

    def load_credentials(self, credential_filepath: str) -> Union[str, None]:
        """Load the credential file from the given filepath.

        Parameters
        ----------
        credential_filepath : str or path-like object

        Returns
        -------
        credentials : str or None

        Raises
        ------
        ValueError
            If credential_filepath is not of type str.
        FileNotFoundError
            Credentials file does not exist.

        See Also
        --------
        os.path : Module for functions on pathnames.
        """

        if not isinstance(credential_filepath, str):
            raise TypeError(
                (f'Credential value must be of type str, '
                    f'not \'{type(credential_filepath)}\'.')
            )

        # Try to load credentials from file
        if os.path.exists(credential_filepath):
            with open(credential_filepath) as credential_file:
                credential_data = json.load(credential_file)

                credentials = credential_data.get(self.repository_name)

                if not credentials:
                    self.queue.put(
                        'No credentials found, attempting unauthorized run.'
                    )
                return credentials
        else:
            raise FileNotFoundError(f'{credential_filepath} does not exist.')

    @staticmethod
    def accepts_user_credentials() -> NoReturn:
        raise NotImplementedError(
            'Subclass must implement "accepts_user_credentials()".'
        )

    @staticmethod
    def _all_empty(data_dict: QueryResultDict) -> bool:
        """Check if all DataFrames are empty.

        Parameters
        ----------
        data_dict : dict from search params to pandas.DataFrame.

        Returns
        -------
        all_empty : bool
        """

        all_empty = False

        for df in data_dict.values():
            if df is None or df.empty:
                all_empty = True

        return all_empty

    def run(self, **kwargs: Any) -> None:
        """Queries all data from the implemented API.

        Parameters
        ----------
        **kwargs : dict, optional
            Can temporarily overwrite self attributes.

        Notes
        -----
        In the following order, this function calls:
            get_all_search_outputs
            get_all_metadata (if applicable)
            merge_search_and_metadata_dicts (if applicable)
        """

        self.queue.put(f'Running {self.repository_name}...')

        # Get search_output
        search_dict = self.get_all_search_outputs(**kwargs)

        # Set merge parameters
        merge_on = vars(self).get('merge_on')
        merge_right_on = vars(self).get('merge_right_on')
        merge_left_on = vars(self).get('merge_left_on')

        # Set save parameters
        save_dir = kwargs.get('save_dir')
        save_csv = kwargs.get('save_csv')
        save_json = kwargs.get('save_json')

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
        except (AttributeError, TypeError):
            # Attribute Error: Tries to call a function that does not exist
            # TypeError: Tries to call function with incorrect arguments
            final_dict = search_dict

        # Handle saving if output exists
        if not self._all_empty(final_dict):
            self.queue.put(f'Saving output to "{save_dir}".')
            if save_csv:
                pycurator.utils.save_results(
                    final_dict,
                    save_dir,
                    extension='csv'
                )
            if save_json:
                pycurator.utils.save_results(
                    final_dict,
                    save_dir,
                    extension='json'
                )
            self.queue.put('Save complete.')
        else:
            self.queue.put('No results found, nothing to save.')

        self.queue.put(f'{self.repository_name} run complete.')
        self.continue_running = False

    def get_all_search_outputs(self, **kwargs: Any) -> NoReturn:
        raise NotImplementedError(
            'Scraper subclasses must override "get_all_search_outputs(...)".'
        )

    def get_all_metadata(self, **kwargs: Any) -> NoReturn:
        raise NotImplementedError(
            'Scraper subclasses must override "get_all_metadata(...)".'
        )

    def get_request_output_and_update_query_ref(
            self,
            url: AnyStr,
            params: Optional[Any] = None,
            headers: Optional[Any] = None,
            **ref_kwargs: Any
    ) -> tuple[requests.Response, JSONDict]:
        """Return request output and update self.current_query_ref.

        Parameters
        ----------
        url : str
        params : dict, optional (default=None)
        headers : dict, optional (default=None)
        **ref_kwargs : dict, optional

        Returns
        -------
        self.get_request_output(url, params, headers)

        See Also
        --------
        _update_query_ref
        get_request_output
        """

        self._update_query_ref(**ref_kwargs)
        return self.get_request_output(url, params, headers)

    def get_request_output(
            self,
            url: AnyStr,
            params: Optional[Any] = None,
            headers: Optional[Any] = None
    ) -> tuple[requests.Response, JSONDict]:
        """Performs a requests.get(...) call, returns response and json.

        Parameters
        ----------
        url : str
        params : dict or list of tuples or bytes, optional (default=None)
            Dictionary, list of types or bytes to send in the query
            string for the Request.
        headers : dict, optional (default=None)
            Dictionary of headers to send with the Request.

        Returns
        -------
        r : requests.Response
        output : dict
            JSON-encoded content of a response.

        Raises
        ------
        RuntimeError
            Occurs when a query results in an unparsable response. Outputs
            the parameters provided to the query along with the response
            status code for further troubleshooting.

        See Also
        --------
        requests.get : Sends a GET request.

        Examples
        --------
        >>> self.get_request_output('')
        """

        # If user has requested termination, handle cleanup instead of querying
        # additional results
        if not self.continue_running:
            self.terminate()

        r = requests.get(url, params=params, headers=headers)
        try:
            output = r.json()
        except json.decoder.JSONDecodeError:
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
                raise RuntimeError(
                    (f'Query to {url} with {params} params and {headers}'
                     f' headers fails unexpectedly with status'
                     f' code {r.status_code} and full output {vars(r)}')
                )

        return r, output

    def merge_search_and_metadata_dicts(
            self,
            search_dict: QueryResultDict,
            metadata_dict: dict,
            on: Optional[Union[Hashable, Sequence[Hashable]]] = None,
            left_on: Optional[Union[Hashable, Sequence[Hashable]]] = None,
            right_on: Optional[Union[Hashable, Sequence[Hashable]]] = None,
            **kwargs: Any
    ) -> QueryResultDict:
        """Merges together search and metadata DataFrames by 'on' key.

        For multiple DataFrames containing similar search references, combines
        into one DataFrame. Search and Metadata DataFrames are merged across
        their respective dictionaries via common keys. For Search DataFrames
        with no matching Metadata, the Search DataFrame added as-is.

        Parameters
        ----------
        search_dict : dict of pandas.DataFrame
            Dictionary of search output results.
        metadata_dict : dict of pandas.DataFrame
            Dictionary of metadata results.
        on : str or list-like, optional (default=None)
            Column name(s) to merge the two dicts on.
        left_on : str or list-like, optional (default=None)
            Column name(s) to merge the left dict on.
        right_on : str or list-like, optional (default=None)
            Column name(s) to merge the right dict on.
        **kwargs : dict, optional
            Placeholder argument to allow inheritance overloading.

        Returns
        -------
        df_dict : dict of pandas.DataFrame
            Dict containing all the merged search/metadata DataFrames
            or singleton search DataFrames.

        Raises
        ------
        TypeError
            search_dict or metadata_dict are not instances of dict.
        ValueError
            search_dict or metadata_dict contain entries that are not of type
            pandas.DataFrame.

        See Also
        --------
        pandas.merge
        """

        if not isinstance(search_dict, dict):
            raise TypeError(
                ('search_dict must be of type dict, not'
                 f'\'{type(search_dict)}\'.')
            )
        if not isinstance(metadata_dict, dict):
            raise TypeError(
                ('metadata_dict must be of type dict, not'
                 f' \'{type(metadata_dict)}\'.')
            )

        if not all(
                [
                    isinstance(df, pd.DataFrame) or df is None
                    for df in search_dict
                ]
        ):
            print([type(df) for df in search_dict])
            raise ValueError(
                'All search_dict entries must be of type pandas.DataFrame.'
            )
        if not all([isinstance(df, pd.DataFrame) for df in metadata_dict]):
            raise ValueError(
                'All metadata_dict entries must be of type pandas.DataFrame.'
            )

        df_dict = dict()
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


class TermScraperMixin:
    @property
    def search_terms(self) -> Collection[SearchTerm]:
        return self._search_terms

    @search_terms.setter
    def search_terms(self, search_terms: Collection[SearchTerm]) -> None:
        if isinstance(search_terms, str):
            search_terms = [search_terms]
        if not all([isinstance(term, str) for term in search_terms]):
            raise TypeError('All search terms must be of type str.')
        self._search_terms = search_terms


class AbstractTermScraper(TermScraperMixin, AbstractAPIScraper):
    """Base Class for scraping repository APIs based on search term.

    Parameters
    ----------
    repository_name : str
        Name of the repository being scraped. Used for loading credentials and
        saving output results.
    search_terms : list-like, optional (default=None)
        Terms to search over. Can be (re)set via set_search_terms() or passed
        in directly to search functions to override set parameter.
    flatten_output : bool, optional (default=False)
        Flag for specifying if nested output should be flattened. Can be passed
        in directly to functions to override set parameter.
    credentials : str, optional (default=None)
        JSON filepath containing credentials in form {repository_name}: 'key'.
    """

    def __init__(
            self,
            repository_name: str,
            search_terms: Optional[Collection[SearchTerm]] = None,
            flatten_output: bool = False,
            credentials: Optional[str] = None
    ) -> None:
        super().__init__(repository_name, flatten_output, credentials)

        self.search_terms = search_terms

    def get_all_search_outputs(
            self,
            **kwargs: Any
    ) -> TermResultDict:
        """Queries the API for each search term.

        Parameters
        ----------
        **kwargs : dict, optional
            Can temporarily overwrite self search_terms and flatten_output
            arguments.

        Returns
        -------
        search_dict : dict of pandas.DataFrame
            Stores the results of each call to get_individual_search_output in
            the form search_dict[{search_term}] = df.
        """

        # Set method variables if different than default values
        search_terms = kwargs.get('search_terms', self.search_terms)
        flatten_output = kwargs.get('flatten_output', self.flatten_output)

        search_dict = dict()

        for search_term in search_terms:
            self.queue.put(f'Searching {search_term}.')
            search_dict[search_term] = self.get_individual_search_output(
                search_term,
                flatten_output=flatten_output
            )
            self.queue.put('Search completed.')

        return search_dict

    @abstractmethod
    def get_individual_search_output(
            self,
            search_term: SearchTerm,
            **kwargs: Any
    ) -> None:
        pass

    def get_all_metadata(
            self,
            object_path_dict: dict[SearchTerm, pd.DataFrame],
            **kwargs: Any
    ) -> TermResultDict:
        """Retrieves all metadata related to the provided DataFrames.

        Parameters
        ----------
        object_path_dict : dict
            Dictionary of the form {query: object_paths} for list of paths.
        **kwargs : dict, optional
            Can temporarily overwrite self flatten_output argument.

        Returns
        -------
        metadata_dict : dict of {SearchTerm: pd.DataFrame}
        """

        flatten_output = kwargs.get('flatten_output', self.flatten_output)

        metadata_dict = dict()

        for query, object_paths in object_path_dict.items():
            self.queue.put(f'Querying {query} metadata.')
            metadata_dict[query] = self.get_query_metadata(
                object_paths,
                flatten_output=flatten_output
            )
            self.queue.put('Metadata query complete.')

        return metadata_dict

    def get_query_metadata(
            self,
            object_paths: Iterable[Any],
            **kwargs: Any
    ) -> NoReturn:
        raise NotImplementedError


class TypeScraperMixin:
    @property
    def search_types(self) -> tuple[SearchType]:
        return self._search_types

    @search_types.setter
    def search_types(self, search_types: tuple[SearchType]) -> None:
        if not all(
                [
                    search_type in self.search_type_options
                    for search_type in search_types
                ]
        ):
            raise ValueError(
                f'Only {self.search_type_options} search types are valid.'
            )
        self._search_types = search_types

    @classmethod
    @abstractmethod
    def search_type_options(cls) -> NoReturn:
        """Return the valid search type options for a given repository."""
        raise NotImplementedError


class AbstractTermTypeScraper(
    TermScraperMixin,
    TypeScraperMixin,
    AbstractAPIScraper
):
    """Base Class for scraping repository APIs based on search term and type.

    Parameters
    ----------
    repository_name : str
        Name of the repository being scraped. Used for loading credentials and
        saving output results.
    search_terms : list-like, optional (default=None)
        Terms to search over. Can be (re)set via set_search_terms() or passed
        in directly to search functions to override set parameter.
    search_types : list-like, optional (default=None)
        Data types to search over. Can be (re)set via set_search_types() or
        passed in directly to search functions to override set parameter.
    flatten_output : bool, optional (default=False)
        Flag for specifying if nested output should be flattened. Can be passed
        in directly to functions to override set parameter.
    credentials : str, optional (default=None)
        JSON filepath containing credentials in form {repository_name}: 'key'.
    """

    def __init__(
            self,
            repository_name: str,
            search_terms: Optional[Collection[SearchTerm]] = None,
            search_types: Optional[Collection[SearchType]] = None,
            flatten_output: bool = False,
            credentials: Optional[str] = None
    ) -> None:
        super().__init__(repository_name, flatten_output, credentials)

        self.search_terms = search_terms
        self.search_types = search_types

    def get_all_search_outputs(
            self,
            **kwargs: Any
    ) -> TermTypeResultDict:
        """Queries the API for each search term/type combination.

        Parameters
        ----------
        **kwargs : dict, optional
            Can temporarily overwrite self search_terms, search_types, and
            flatten_output arguments.

        Returns
        -------
        search_dict : dict of pandas.DataFrame
            Stores the results of each call to get_individual_search_output in
            the form search_dict[(search_term, search_type)] = df.
        """

        # Set method variables if different than default values.
        search_terms = kwargs.get('search_terms', self.search_terms)
        search_types = kwargs.get('search_types', self.search_types)
        flatten_output = kwargs.get('flatten_output', self.flatten_output)

        search_dict = dict()

        for search_term, search_type in itertools.product(
                search_terms, search_types
        ):
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
    def get_individual_search_output(
            self,
            search_term: SearchTerm,
            search_type: SearchType,
            **kwargs: Any
    ) -> None:
        pass

    def get_all_metadata(
            self,
            object_path_dict: dict[SearchTuple, Collection[str]],
            **kwargs: Any
    ) -> TermTypeResultDict:
        """Retrieves all metadata that relates to the provided DataFrames.

        Parameters
        ----------
        object_path_dict : dict
            Dictionary of the form {query: object_paths} for list of paths.
        **kwargs : dict, optional
            Can temporarily overwrite self flatten_output argument.

        Returns
        -------
        metadata_dict : dict of {(SearchTerm, SearchType): pd.DataFrame}
        """

        flatten_output = kwargs.get('flatten_output', self.flatten_output)

        metadata_dict = dict()

        for query, object_paths in object_path_dict.items():
            search_term, search_type = query
            self.queue.put(f'Querying {search_term} {search_type} metadata.')

            metadata_dict[query] = self.get_query_metadata(
                object_paths=object_paths,
                flatten_output=flatten_output
            )
            self.queue.put('Metadata query complete.')

        return metadata_dict

    def get_query_metadata(
            self,
            object_paths: Collection[str],
            **kwargs: Any
    ) -> NoReturn:
        raise NotImplementedError


class AbstractTypeScraper(TypeScraperMixin, AbstractAPIScraper):
    """Base Class for scraping repository APIs based on search type.

    Parameters
    ----------
    repository_name : str
        Name of the repository being scraped. Used for loading credentials and
        saving output results.
        Web scrapers do not require user credentials at all.
    search_types : list-like, optional (default=None)
        types to search over. Can be (re)set via set_search_types() or passed
        in directly to search functions to override set parameter.
    flatten_output : bool, optional (default=False)
        Flag for specifying if nested output should be flattened. Can be passed
        in directly to functions to override set parameter.
    credentials : str, optional (default=None)
        JSON filepath containing credentials in form {repository_name}: 'key'.
    """

    def __init__(
            self,
            repository_name: str,
            search_types: Optional[Collection[SearchType]] = None,
            flatten_output: bool = False,
            credentials: Optional[str] = None
    ) -> None:
        super().__init__(repository_name, flatten_output, credentials)

        self.search_types = search_types

    @property
    def search_types(self) -> Collection[SearchType]:
        return self._search_types

    @search_types.setter
    def search_types(self, search_types: Collection[SearchType]) -> None:
        if not all(
            [
                search_type in self.search_type_options
                for search_type in search_types
            ]
        ):
            raise ValueError(
                f'Only {self.search_type_options} search types are valid.'
            )
        self._search_types = search_types

    @classmethod
    @abstractmethod
    def search_type_options(cls) -> None:
        """Return the valid search type options for a given repository."""
        pass

    def get_all_search_outputs(
            self,
            **kwargs: Any
    ) -> TermResultDict:
        """Queries the API for each search type.

        Parameters
        ----------
        **kwargs : dict, optional
            Can temporarily overwrite self search_types and flatten_output
            arguments.

        Returns
        -------
        search_dict : dict of pandas.DataFrame
            Stores the results of each call to get_individual_search_output in
            the form search_output_dict[{search_type}] = df.
        """

        search_types = kwargs.get('search_types', self.search_types)
        flatten_output = kwargs.get('flatten_output', self.flatten_output)

        search_dict = dict()

        for search_type in search_types:
            self.queue.put(f'Searching {search_type}.')
            search_dict[search_type] = self.get_individual_search_output(
                search_type,
                flatten_output=flatten_output
            )
            self.queue.put(f'{search_type} search completed.')

        return search_dict

    @abstractmethod
    def get_individual_search_output(
            self,
            search_type: SearchType,
            **kwargs: Any
    ) -> None:
        pass

    def get_query_metadata(
            self,
            object_paths: Collection[str],
            search_type: SearchType,
            **kwargs: Any
    ) -> NoReturn:
        raise NotImplementedError
