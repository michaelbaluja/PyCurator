"""
Module for collecting data from Dataverse repository.
"""

from collections.abc import Collection
from typing import Optional, Union, NoReturn

import pandas as pd

from ..base import (
    BaseCollector,
    BaseTermTypeCollector,
)
from pycurator._typing import (
    SearchTerm,
    SearchType,
)


class DataverseCollector(BaseTermTypeCollector):
    """Harvard Dataverse collector for search term and type queries.

    Parameters
    ----------
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

    accepts_credentials: bool = True
    search_type_options: tuple[SearchType, ...] = ("dataset", "file")
    api_url: str = "https://dataverse.harvard.edu/api"

    def __init__(
            self,
            search_terms: Optional[Collection[SearchTerm]] = None,
            search_types: Optional[Collection[SearchType]] = None,
            credentials: Optional[str] = None,
    ) -> None:

        super().__init__("dataverse", search_terms=search_terms, search_types=search_types)
        self.headers = {}

        if credentials:
            self.credentials = self.load_credentials(credential_filepath=credentials)

    def get_query_metadata(self, object_paths: Collection[str]) -> NoReturn:
        pass

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

        credentials = super().load_credentials(credential_filepath=credential_filepath)
        self.headers["X-Dataverse-key"] = credentials
        return credentials

    @BaseCollector.track_indeterminate_progress
    @BaseTermTypeCollector.validate_term_and_type
    def get_individual_search_output(
            self, search_term: SearchTerm, search_type: SearchType
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

        search_url = f"{self.api_url}/search"

        # Set search parameters
        start = 0
        page_size = 25
        search_df = pd.DataFrame()
        page_idx = 0

        search_params = {
            "q": search_term,
            "per_page": page_size,
            "start": start,
            "type": search_type,
        }

        # Conduct initial search & extract results
        _, output = self.get_request_output_and_update_query_ref(
            url=search_url,
            params=search_params,
            headers=self.headers,
            search_term=search_term,
            search_type=search_type,
            page=page_idx,
        )
        output = output["data"]

        while output.get("items"):
            output = output["items"]

            output_df = pd.DataFrame(output)
            output_df["page"] = search_params["start"] // search_params["per_page"] + 1

            search_df = pd.concat([search_df, output_df]).reset_index(drop=True)

            search_params["start"] += search_params["per_page"]
            page_idx += 1

            _, output = self.get_request_output_and_update_query_ref(
                url=search_url,
                params=search_params,
                headers=self.headers,
                search_term=search_term,
                search_type=search_type,
                page=page_idx,
            )
            output = output["data"]

        return search_df
