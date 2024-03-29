"""
Module containing abstract base for PyCurator collector classes.
"""

import itertools
import json
import os
import queue
import sys
import time
from abc import ABC, abstractmethod
from collections.abc import (
    Collection,
    Callable,
    Generator,
    Iterable,
)
from typing import Any, AnyStr, NoReturn, Optional, ParamSpec, TypeVar, Union

import pandas as pd
import requests

from .utils.saving import save_results
from .utils.validating import is_all_type, validate_from_arguments
from pycurator._typing import (
    JSONDict,
    SearchTerm,
    SearchType,
    SearchTuple,
    TermResultDict,
    TermTypeResultDict,
    QueryResultDict,
)

T = TypeVar("T")
P = ParamSpec("P")


class BaseCollector(ABC):
    """Generic abstract base for data-collection classes.

    Parameters
    ----------
    repository_name : str
        Name of the repository being collected from. Used for providing
        updates to user, loading credentials, and saving output results.

    Attributes
    ----------
    continue_running : bool
        Flag when the collector completes a run. Used for pushing
        updates from the object's status_queue.
    current_query_ref : str or None (default=None)
        Representation of the current state of the collector run.
    num_queries : int or bool or None (default=None)
        Number of queries for a given run.
        If there is no fixed number, such as for paginated queries, the
        variable is True.
    queries_completed : int or None (default=None)
    status_queue : queue.Queue of str
        FIFO collection of the object's status.

    See Also
    --------
    queue.Queue : Queue data structure
    BaseAPICollector : Derived Class for API queries.
    """

    def __init__(self, repository_name: str) -> None:
        self.repository_name = repository_name
        self.continue_running = True

        self.status_queue = queue.Queue()
        self.num_queries = None
        self.queries_completed = None
        self.current_query_ref = None

    def track_determinate_progress(
            self, coll: Collection[T]
    ) -> Generator[T, None, None]:
        """Generator for iterating data and updating progress bar.

        Parameters
        ----------
        coll : iterable

        Yields
        ------
        next object of coll

        Raises
        ------
        TypeError
            coll parameter is not iterable.
        """

        if not hasattr(coll, "__iter__"):
            raise TypeError('Parameter "coll" must be iterable.')

        # Initialize tracking vars
        if not self.num_queries:
            self.num_queries = len(coll)
            self.queries_completed = 0

        # Yield next item in coll and update tracking vars
        for item in coll:
            self.current_query_ref = str(item)
            yield item
            self.queries_completed += 1

        # Reset tracking vars
        self.num_queries = None
        self.queries_completed = None
        self.current_query_ref = None

    def _save_results(
            self, save_dir: str, final_dict: QueryResultDict, output_format: str
    ) -> None:
        """Helper function for saving results and reporting status to UI."""
        self.status_queue.put(f'Saving output to "{save_dir}".')
        save_results(results=final_dict, data_dir=save_dir, output_format=output_format)
        self.status_queue.put("Save complete.")

    @abstractmethod
    def run(self) -> NoReturn:
        """Abstract placeholder method for collector run."""
        raise NotImplementedError('Subclass must override "run()".')

    @staticmethod
    def track_indeterminate_progress(
            indeterminate_query_func: Callable[P, T]
    ) -> Callable[P, T]:
        """Progress bar wrapper for indeterminate-length queries."""

        def update_pb(self, *args: P.args, **kwargs: P.kwargs) -> Iterable[Any]:
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
        self.status_queue.put("Requesting program termination.")
        sys.exit()

    def _print_progress(self, page: str) -> None:
        """Update queue with current page being searched."""
        self.status_queue.put(f"Searching page {page}")

    def _update_query_ref(self, **kwargs: Any) -> None:
        """Combine keywords and update base.current_query_ref."""
        self.current_query_ref = kwargs


class BaseAPICollector(BaseCollector):
    """Base for collection classes utilizing external API.

    This base inherits from BaseCollector, which provides general
    parameters for tracking collection progress.

    Parameters
    ----------
    repository_name : str
        Name of the repository being collected from. Used for providing
        updates to user, loading credentials, and saving output results.
    credentials : str, optional (default=None)
        JSON filepath containing credentials in form
        {repository_name}: {key}.

    Attributes
    ----------
    credentials : str
        Refer to credentials parameter.

    See Also
    --------
    BaseTermCollector
    BaseTermTypeCollector
    BaseTypeCollector
    """

    def __init__(self, repository_name: str, credentials: Optional[str] = None) -> None:
        super().__init__(repository_name=repository_name)

        if credentials:
            self.credentials = self.load_credentials(credential_filepath=credentials)

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
                (
                    f"Credential value must be of type str, "
                    f"not '{type(credential_filepath)}'."
                )
            )

        # Try to load credentials from file
        if os.path.exists(credential_filepath):
            with open(credential_filepath) as credential_file:
                credential_data = json.load(credential_file)

                credentials = credential_data.get(self.repository_name)

                if not credentials:
                    self.status_queue.put(
                        "No credentials found, attempting unauthorized run."
                    )
                return credentials
        else:
            raise FileNotFoundError(f"{credential_filepath} does not exist.")

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

        return all(df is None or df.empty for df in data_dict.values())

    def run(self, **kwargs: Any) -> None:
        """Queries all data from the implemented API.

        Parameters
        ----------
        **kwargs : dict, optional
            Can temporarily overwrite base attributes.
            Allows users to specify variable save parameters.

        Notes
        -----
        In the following order, this function calls:
            get_all_search_outputs
            get_all_metadata (if applicable)
            merge_search_and_metadata_dicts (if applicable)
        """

        self.status_queue.put(f"Running {self.repository_name.title()}...")

        # Set save parameters
        save_dir = kwargs.pop("save_dir", None)
        save_type = kwargs.pop("save_type", None)

        try:
            # Get search_output
            search_dict = self.get_all_search_outputs(**kwargs)

            # Set merge parameters
            merge_kwargs = {}
            try:
                merge_kwargs["on"] = getattr(self, "merge_on")
            except AttributeError:
                try:
                    merge_kwargs["left_on"] = getattr(self, "merge_left_on")
                    merge_kwargs["right_on"] = getattr(self, "merge_right_on")
                except AttributeError:
                    raise NotImplementedError(
                        f"Class '{self.__name__}' must include 'merge_on' or both 'merge_left_on' and 'merge_right_on'"
                    )

            # Try to get metadata (if available)
            try:
                metadata_dict = self.get_all_metadata(search_dict=search_dict)
                merged_dict = self.merge_search_and_metadata_dicts(
                    search_dict=search_dict,
                    metadata_dict=metadata_dict,
                    **merge_kwargs
                )
                final_dict = merged_dict
            except TypeError:
                final_dict = search_dict
        except Exception as unexpected_error:
            self.status_queue.put(
                f"An unexpected error has occurred: \n{unexpected_error}"
            )
            self.continue_running = False
            return

        # Handle saving if output exists
        if not self._all_empty(final_dict):
            if save_dir and save_type:
                self._save_results(
                    save_dir=save_dir, final_dict=final_dict, output_format=save_type
                )
        else:
            self.status_queue.put("No results found, nothing to save.")

        self.status_queue.put(f"{self.repository_name.title()} run complete.")
        self.continue_running = False

    def get_all_search_outputs(self, **kwargs: Any) -> NoReturn:
        """Abstract placeholder method for returning search outputs."""
        raise NotImplementedError('Subclass must override "get_all_search_outputs()".')

    def get_all_metadata(self, search_dict: QueryResultDict) -> NoReturn:
        """Abstract placeholder method for returning metadata."""
        raise NotImplementedError('Subclass must override "get_all_metadata()".')

    def get_request_output_and_update_query_ref(
            self,
            url: AnyStr,
            params: Optional[Any] = None,
            headers: Optional[Any] = None,
            **ref_kwargs: Any,
    ) -> tuple[requests.Response, JSONDict]:
        """Return request output and update base.current_query_ref.

        Parameters
        ----------
        url : str
        params : dict, optional (default=None)
        headers : dict, optional (default=None)
        **ref_kwargs : dict, optional

        Returns
        -------
        base.get_request_output(url, params, headers)

        See Also
        --------
        _update_query_ref
        get_request_output
        """

        self._update_query_ref(**ref_kwargs)
        return self.get_request_output(url=url, params=params, headers=headers)

    def get_request_output(
            self, url: AnyStr, params: Optional[Any] = None, headers: Optional[Any] = None
    ) -> tuple[requests.Response, JSONDict]:
        """Return Response and JSON from requests.get().

        Parameters
        ----------
        url : str
        params : dict or list of tuples or bytes, optional
                (default=None)
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
            Occurs when a query results in an un-parsable response.
            Outputs the parameters provided to the query along with the
            response status code for further troubleshooting.

        See Also
        --------
        requests.get : Sends a GET request.
        """

        # If user has requested termination, handle cleanup instead of querying
        # additional results
        if not self.continue_running:
            self.terminate()

        request_obj = requests.get(url=url, params=params, headers=headers)
        try:
            output = request_obj.json()
        except json.JSONDecodeError as invalid_json:
            # 429: Rate limiting (wait and then try the request again)
            if request_obj.status_code == 429:
                self.status_queue.put("Rate limit hit, waiting for request...")

                # Wait until we can make another request
                reset_time = int(request_obj.headers["RateLimit-Reset"])
                current_time = int(time.time())
                time.sleep(reset_time - current_time)

                request_obj, output = self.get_request_output(
                    url=url, params=params, headers=headers
                )
            else:
                raise RuntimeError(
                    (
                        f"Query to {url} with {params} params and {headers}"
                        f" headers fails unexpectedly with status"
                        f" code {request_obj.status_code} and full output {vars(request_obj)}"
                    )
                ) from invalid_json

        return request_obj, output

    def merge_search_and_metadata_dicts(
            self,
            search_dict: QueryResultDict,
            metadata_dict: dict,
            **kwargs: Any,
    ) -> QueryResultDict:
        """Merges together search and metadata DataFrames by 'on' key.

        For multiple DataFrames containing similar search references,
        combines into one DataFrame. Search and Metadata DataFrames are
        merged across their respective dictionaries via common keys.
        For Search DataFrames with no matching Metadata, the Search
        DataFrame is added as-is.

        Parameters
        ----------
        search_dict : dict of pandas.DataFrame
            Dictionary of search output results.
        metadata_dict : dict of pandas.DataFrame
            Dictionary of metadata results.
        **kwargs : dict, optional
            Additional keyword arguments to pass to merge.

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
            search_dict or metadata_dict contain entries that are not of
            type pandas.DataFrame.

        See Also
        --------
        pandas.merge
        """

        if not isinstance(search_dict, dict):
            raise TypeError(
                f"search_dict must be of type dict, not '{type(search_dict)}'."
            )
        if not isinstance(metadata_dict, dict):
            raise TypeError(
                f"metadata_dict must be of type dict, not '{type(metadata_dict)}'."
            )

        if not is_all_type(search_dict.values(), (pd.DataFrame, None)):
            raise ValueError(
                "All search_dict entries must be of type pandas.DataFrame."
            )
        if not is_all_type(metadata_dict.values(), (pd.DataFrame, None)):
            raise ValueError(
                "All metadata_dict entries must be of type pandas.DataFrame."
            )

        df_dict = {}
        for query_key in search_dict.keys():
            search_df = search_dict[query_key]

            # If the search DataFrame has matching metadata, merge
            if query_key in metadata_dict:
                metadata_df = metadata_dict[query_key]
                df_all = pd.merge(
                    left=search_df,
                    right=metadata_df,
                    how="outer",
                    suffixes=("_search", "_metadata"),
                    **kwargs,
                )
            # If no metadata, just add the search_df
            else:
                df_all = search_df

            df_dict[query_key] = df_all

        return df_dict


class TermQueryMixin:
    """Mixin for API collection classes that utilize search terms.

    See Also
    --------
    BaseTermCollector
    BaseTermTypeCollector
    """

    _search_terms: Collection[SearchTerm]

    @property
    def search_terms(self) -> Collection[SearchTerm]:
        """Property method for search terms."""
        return self._search_terms

    @staticmethod
    def validate_search_term(func: Callable[P, T]) -> Callable[P, T]:
        """Decorator for validating search term object type."""

        def inner(self, *args, **kwargs):
            args, kwargs = validate_from_arguments(
                validator=self._validate,
                func=func,
                args=args,
                kwargs=kwargs,
                param="search_term",
            )
            return func(self, *args, **kwargs)

        return inner

    @search_terms.setter
    def search_terms(self, search_terms: Collection[SearchTerm]) -> None:
        if isinstance(search_terms, str):
            search_terms = [search_terms]
        if not is_all_type(search_terms, str):
            raise TypeError("All search terms must be of type str.")
        self._search_terms = search_terms

    @staticmethod
    def _validate(search_term: SearchTerm) -> SearchTerm:
        """Validate type of search_term."""
        if not isinstance(search_term, str):
            raise TypeError(
                "search_term must be of type str, not" f" '{type(search_term)}'."
            )
        return search_term


class BaseTermCollector(TermQueryMixin, BaseAPICollector):
    """Base for API collection classes that utilize search terms.

    This base inherits from BaseAPICollector, which provides credential
    info, as well as general parameters for tracking collection
    progress, inherited from BaseCollector.

    Parameters
    ----------
    repository_name : str
        Name of the repository being collected from. Used for providing
        updates to user, loading credentials, and saving output results.
    search_terms : list-like, optional (default=None)
        Terms to search over. Can be (re)set via set_search_terms() or
        passed in directly to search functions to override set parameter.
    credentials : str, optional (default=None)
        JSON filepath containing credentials in form
        {repository_name}: {key}.

    Attributes
    ----------
    search_terms : list of str

    See Also
    --------
    pycurator.collectors.term_collectors
    """

    def __init__(
            self,
            repository_name: str,
            search_terms: Optional[Collection[SearchTerm]] = None,
            credentials: Optional[str] = None,
    ) -> None:
        super().__init__(repository_name=repository_name, credentials=credentials)

        self.search_terms = search_terms

    def get_all_search_outputs(self, **kwargs: Any) -> TermResultDict:
        """Queries the API for each search term.

        Parameters
        ----------
        **kwargs : dict, optional
            Can temporarily overwrite self search_terms.

        Returns
        -------
        search_dict : dict of pandas.DataFrame
            Stores the results of each call to
            get_individual_search_output in the form
            search_dict[{search_term}] = df.
        """

        # Set method variables if different than default values
        search_terms = kwargs.get("search_terms", self.search_terms)

        search_dict = {}

        for search_term in search_terms:
            self.status_queue.put(f"Searching {search_term}.")
            search_dict[search_term] = self.get_individual_search_output(
                search_term=search_term
            )
            self.status_queue.put("Search completed.")

        return search_dict

    @abstractmethod
    def get_individual_search_output(self, search_term: SearchTerm) -> None:
        """Abstract placeholder method for search output."""
        return

    def _get_metadata_from_paths(
            self, object_path_dict: dict[SearchTerm, pd.DataFrame]
    ) -> TermResultDict:
        """Retrieves all metadata related to the provided DataFrames.

        Parameters
        ----------
        object_path_dict : dict
            Dictionary of the form {query: object_paths} for path lists.

        Returns
        -------
        metadata_dict : dict of {SearchTerm: pd.DataFrame}
        """

        metadata_dict = {}

        for query, object_paths in object_path_dict.items():
            self.status_queue.put(f"Querying {query} metadata.")
            metadata_dict[query] = self.get_query_metadata(object_paths=object_paths)
            self.status_queue.put("Metadata query complete.")

        return metadata_dict

    def get_query_metadata(self, object_paths: Iterable[Any]) -> NoReturn:
        """Placeholder method for query metadata retrieval."""
        raise NotImplementedError('Subclass must override "get_query_metadata()".')


class TypeQueryMixin:
    """Mixin for API collection classes that utilize search types.

    See Also
    --------
    BaseTermTypeCollector
    BaseTypeCollector
    """

    _search_types: tuple[SearchType, ...]
    search_type_options: tuple[SearchType, ...] = None

    @property
    def search_types(self) -> tuple[SearchType, ...]:
        """Getter for search_types."""
        return self._search_types

    @search_types.setter
    def search_types(self, search_types: tuple[SearchType, ...]) -> None:
        """Set search_types if all are allowed by current Collector."""
        if not all(
                search_type in self.search_type_options for search_type in search_types
        ):
            raise ValueError(f"Only {self.search_type_options} search types are valid.")
        self._search_types = search_types

    @staticmethod
    def validate_search_type(func: Callable[P, T]) -> Callable[P, T]:
        """Decorator for validating search term object type."""

        def inner(self, *args, **kwargs):
            args, kwargs = validate_from_arguments(
                validator=self._validate,
                func=func,
                args=args,
                kwargs=kwargs,
                param="search_type",
            )
            return func(self, *args, **kwargs)

        return inner

    def _validate(self, search_type: SearchType) -> SearchType:
        if search_type not in self.search_type_options:
            raise ValueError(f"Can only search by {self.search_type_options}.")
        return search_type


class BaseTermTypeCollector(TermQueryMixin, TypeQueryMixin, BaseAPICollector):
    """Base for API collection classes that utilize search terms and types.

    This base inherits from BaseAPICollector, which provides credential
    info, as well as general parameters for tracking collection
    progress, inherited from BaseCollector.

    Parameters
    ----------
    repository_name : str
        Name of the repository being collected from. Used for providing
        updates to user, loading credentials, and saving output results.
    search_terms : list-like, optional (default=None)
        Terms to search over. Can be (re)set via set_search_terms() or
        passed in directly to search functions to override set parameter.
    search_types : list-like, optional (default=None)
        Data types to search over. Can be (re)set via set_search_types()
        or passed in directly to search functions to override set
        parameter.
    credentials : str, optional (default=None)
        JSON filepath containing credentials in form
        {repository_name}: {key}.

    Attributes
    ----------
    search_terms : list of str
    search_types : list of str

    See Also
    --------
    pycurator.collectors.term_type_collectors
    """

    def __init__(
            self,
            repository_name: str,
            search_terms: Optional[Collection[SearchTerm]] = None,
            search_types: Optional[Collection[SearchType]] = None,
            credentials: Optional[str] = None,
    ) -> None:
        super().__init__(repository_name=repository_name, credentials=credentials)

        self.search_terms = search_terms
        self.search_types = search_types

    @staticmethod
    def validate_term_and_type(func: Callable[P, T]) -> Callable[P, T]:
        """Helper for wrapping function in both term/type validators."""

        @BaseTermTypeCollector.validate_search_term
        @BaseTermTypeCollector.validate_search_type
        def inner(self, *args, **kwargs):
            return func(self, *args, **kwargs)

        return inner

    def get_all_search_outputs(self, **kwargs: Any) -> TermTypeResultDict:
        """Queries the API for each search term/type combination.

        Parameters
        ----------
        **kwargs : dict, optional
            Can temporarily overwrite self search_terms and
            search_types.

        Returns
        -------
        search_dict : dict of pandas.DataFrame
            Stores the results of each call to
            get_individual_search_output in the form
            search_dict[(search_term, search_type)] = df.
        """

        # Set method variables if different than default values.
        search_terms = kwargs.get("search_terms", self.search_terms)
        search_types = kwargs.get("search_types", self.search_types)

        search_dict = {}

        for search_term, search_type in itertools.product(search_terms, search_types):
            self.status_queue.put(f"Searching {search_term} {search_type}.")
            search_dict[(search_term, search_type)] = self.get_individual_search_output(
                search_term=search_term, search_type=search_type
            )
            self.status_queue.put("Search completed.")

        return search_dict

    @abstractmethod
    def get_individual_search_output(
            self, search_term: SearchTerm, search_type: SearchType
    ) -> None:
        """Abstract placeholder method for retrieving search output."""
        raise NotImplementedError

    def _get_metadata_from_paths(
            self, object_path_dict: dict[SearchTuple, Collection[str]]
    ) -> TermTypeResultDict:
        """Retrieves metadata for records contained in input DataFrames.

        Parameters
        ----------
        object_path_dict : dict
            Dictionary of the form {query: object_paths} for path lists.

        Returns
        -------
        metadata_dict : dict of {(SearchTerm, SearchType): pd.DataFrame}
        """

        metadata_dict = {}

        for query, object_paths in object_path_dict.items():
            search_term, search_type = query
            self.status_queue.put(f"Querying {search_term} {search_type} metadata.")

            metadata_dict[query] = self.get_query_metadata(object_paths=object_paths)
            self.status_queue.put("Metadata query complete.")

        return metadata_dict

    def get_query_metadata(self, object_paths: Collection[str]) -> NoReturn:
        """Placeholder method for query metadata retrieval."""
        raise NotImplementedError('Subclass must override "get_query_metadata()".')


class BaseTypeCollector(TypeQueryMixin, BaseAPICollector):
    """Base for API collection classes that utilize search types.

    This base inherits from BaseAPICollector, which provides credential
    info, as well as general parameters for tracking collection
    progress, inherited from BaseCollector.

    Parameters
    ----------
    repository_name : str
        Name of the repository being collected from. Used for providing
        updates to user, loading credentials, and saving output results.
    search_types : list-like, optional (default=None)
        types to search over. Can be (re)set via set_search_types() or
        passed in directly to search functions to override set parameter.
    credentials : str, optional (default=None)
        JSON filepath containing credentials in form
        {repository_name}: {key}.

    Attributes
    ----------
    search_types : list of str

    See Also
    --------
    pycurator.collectors.type_collectors
    """

    def __init__(
            self,
            repository_name: str,
            search_types: Optional[Collection[SearchType]] = None,
            credentials: Optional[str] = None,
    ) -> None:
        super().__init__(repository_name=repository_name, credentials=credentials)

        self.search_types = search_types

    @property
    def search_types(self) -> Collection[SearchType]:
        """Getter for search_types."""
        return self._search_types

    @search_types.setter
    def search_types(self, search_types: tuple[SearchType, ...]) -> None:
        if not search_types:
            return
        if not all(
                search_type in self.search_type_options for search_type in search_types
        ):
            raise ValueError(f"Only {self.search_type_options} search types are valid.")
        self._search_types = search_types

    def get_all_search_outputs(self, **kwargs: Any) -> TermResultDict:
        """Queries the API for each search type.

        Parameters
        ----------
        **kwargs : dict, optional
            Can temporarily overwrite self search_types.

        Returns
        -------
        search_dict : dict of pandas.DataFrame
            Stores the results of each call to
            get_individual_search_output in the form
            search_output_dict[{search_type}] = df.
        """

        search_types = kwargs.get("search_types", self.search_types)

        search_dict = {}

        for search_type in search_types:
            self.status_queue.put(f"Searching {search_type}.")
            search_dict[search_type] = self.get_individual_search_output(
                search_type=search_type
            )
            self.status_queue.put(f"{search_type} search completed.")

        return search_dict

    @abstractmethod
    def get_individual_search_output(self, search_type: SearchType) -> None:
        """Abstract placeholder method for search output."""
        return

    def get_query_metadata(
            self,
            object_paths: Collection[str],
            search_type: SearchType,
    ) -> NoReturn:
        """Placeholder method for retrieving query metadata."""
        raise NotImplementedError('Subclass must override "get_query_metadata()".')
