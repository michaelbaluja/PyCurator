from collections.abc import Collection
from typing import Any, Optional, Union

import numpy as np
import pandas as pd

from pycurator._typing import (
    SearchTerm,
    TermResultDict
)
from pycurator.collectors import (
    BaseCollector,
    BaseTermCollector,
)
from pycurator.utils.validating import validate_metadata_parameters


class DryadCollector(BaseTermCollector):
    """DataDryad collector for search term queries.

    Parameters
    ----------
    search_terms : list-like, optional
        Terms to search over. Can be (re)set via set_search_terms()
        or passed in directly to search functions.
    credentials : str, optional (default=None)
        JSON filepath containing credentials in form
        {repository_name}: {key}.
    """

    def __init__(
        self,
        search_terms: Optional[Collection[SearchTerm]] = None,
        credentials: Optional[bool] = None,
    ) -> None:

        super().__init__(
            repository_name='dryad',
            search_terms=search_terms,
            credentials=credentials
        )

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
        _, output = self.get_request_output(
            url=search_url,
            params=search_params
        )

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

    @BaseTermCollector.validate_search_term
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
                search_url=search_url,
                search_params=search_params,
                delim='stash:files',
                print_progress=False
            )

            object_df['version'] = object_path
            object_df.loc[:, 'page'] = search_params['page']
            metadata_df = pd.concat(
                [metadata_df, object_df]
            ).reset_index(drop=True)

        return metadata_df

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
            query: self._extract_version_ids(df=df)
            for query, df in search_dict.items()
        }

        metadata_dict = super().get_all_metadata(
            object_path_dict=object_path_dict
        )

        return metadata_dict
