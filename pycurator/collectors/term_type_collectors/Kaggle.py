import json
import os
from collections.abc import Collection
from typing import Optional, Union

import pandas as pd
from kaggle import KaggleApi
from kaggle.rest import ApiException

from pycurator._typing import (
    JSONDict,
    SearchTerm,
    SearchType,
    TermTypeResultDict
)
from pycurator.collectors import (
    BaseCollector,
    BaseTermTypeCollector
)
from pycurator.utils.validating import validate_metadata_parameters


class KaggleCollector(BaseTermTypeCollector):
    """Kaggle collector for search term and type queries.

    Parameters
    ----------
    search_terms : list-like, optional (default=None)
        Terms to search over. Can be (re)set via set_search_terms() or
        passed in directly to search functions.
    search_types : list-like, optional (default=None)
        Data types to search over. Can be (re)set via set_search_types()
        or passed in directly to search functions to override set
        parameter.

    Notes
    -----
    For validating Kaggle requests, read the official documentation on
    authentication at https://www.kaggle.com/docs/api.
    """

    def __init__(
        self,
        search_terms: Optional[Collection[SearchTerm]] = None,
        search_types: Optional[Collection[SearchType]] = None
    ) -> None:

        super().__init__(
            repository_name='kaggle',
            search_terms=search_terms,
            search_types=search_types
        )

        self.api = KaggleApi()
        self.api.authenticate()
        self.merge_on = 'id'

    @staticmethod
    def accepts_user_credentials() -> bool:
        return False

    @classmethod
    @property
    def search_type_options(cls) -> tuple[SearchType, ...]:
        return ('datasets', 'kernels')

    @BaseCollector._pb_indeterminate
    @BaseTermTypeCollector.validate_term_and_type
    def get_individual_search_output(
            self,
            search_term: SearchTerm,
            search_type: SearchType,
    ) -> pd.DataFrame:
        """Queries the Kaggle API for the specified search term and type.

        Parameters
        ----------
        search_term : str
        search_type : {'datasets', 'kernels'}

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

        # Use search type to get relevant API function
        list_queries = getattr(self.api, f'{search_type}_list')

        page_idx = 1
        search_df = pd.DataFrame()

        self._update_query_ref(page='page_idx')
        output = list_queries(search=search_term, page=page_idx)

        while output:
            if not self.continue_running:
                self.terminate()

            if search_type == 'kernels':
                output = [vars(result) for result in output]

            output_df = pd.DataFrame(output)
            output_df['page'] = page_idx

            search_df = pd.concat(
                [search_df, output_df]
            ).reset_index(drop=True)

            page_idx += 1
            self._update_query_ref(page=page_idx)
            output = list_queries(search=search_term, page=page_idx)

        if not search_df.empty:
            search_df = search_df.rename(
                columns={'id': 'datasetId', 'ref': 'id'}
            )

            if search_type == 'datasets':
                search_df = search_df.drop(columns={'viewCount', 'voteCount'})

            search_df = search_df.convert_dtypes()

        return search_df

    def _retrieve_object_json(
            self,
            object_path: str,
            data_path: Optional[str] = f'data{os.sep}'
    ) -> Union[JSONDict, None]:
        """Queries the Kaggle API for metadata JSON file.

        Parameters
        ----------
        object_path : str
        data_path : str, optional (default='data/')
            Location to save metadata to.

        Returns
        -------
        json_data : dict or None

        Raises
        ------
        kaggle.rest.ApiException
            A query was made that was unable to be fulfilled.

        See Also
        --------
        kaggle
        """

        if not self.continue_running:
            self.terminate()

        try:
            self.api.dataset_metadata(dataset=object_path, path=data_path)
        except (TypeError, ApiException) as e:
            if (isinstance(e, ApiException) and e.status != 404
                    and 'bigquery' not in e.headers['Turbolinks-Location']):
                raise e
            else:
                return None
        else:
            metadata_file_path = f'{data_path}dataset-metadata.json'
            with open(metadata_file_path) as f:
                json_data = json.load(f)

            os.remove(metadata_file_path)

            return json_data

    def get_query_metadata(
            self,
            object_paths: Collection[str],
    ) -> pd.DataFrame:
        """Retrieves the metadata for the object_paths objects.

        Parameters
        ----------
        object_paths : str or list-like of str

        Returns
        -------
        metadata_df : pandas.DataFrame
        """

        object_paths = validate_metadata_parameters(object_paths)

        metadata_df = pd.DataFrame()

        for object_path in self._pb_determinate(object_paths):
            json_data = self._retrieve_object_json(object_path=object_path)
            metadata_df = pd.concat(
                [metadata_df, pd.DataFrame(json_data)]
            ).reset_index(drop=True)

        metadata_df = metadata_df.convert_dtypes()

        return metadata_df

    def get_all_metadata(
            self,
            search_dict: TermTypeResultDict
    ) -> TermTypeResultDict:
        """Retrieves metadata for records contained in input DataFrames.

        Parameters
        ----------
        search_dict : dict of pandas.DataFrames
            Output from get_all_search_outputs function.

        Returns
        -------
        metadata_dict : dict of DataFrames
            Stores the results of each call to get_query_metadata in the
            form: metadata_dict[(search_term, search_type)] = df.
        """

        object_path_dict = dict()

        for query, df in search_dict.items():
            # Only want to get metadata for non-empty dataset DataFrames
            if 'kernels' not in query and df is not None:
                # Extract object paths
                object_paths = df.id.values
                object_path_dict[query] = object_paths

        metadata_dict = super().get_all_metadata(
            object_path_dict=object_path_dict
        )

        return metadata_dict
