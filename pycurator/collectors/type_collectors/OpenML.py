import ast
from collections.abc import Collection
from typing import Optional, Union

import openml
import pandas as pd

from pycurator._typing import (
    SearchType,
    TypeResultDict
)
from pycurator.collectors import BaseTypeCollector
from pycurator.utils import validate_metadata_parameters


class OpenMLCollector(BaseTypeCollector):
    """OpenML collector for search type queries.

    Parameters
    ----------
    search_types : list-like, optional
        Types to search over. Can be (re)set via set_search_types() or
        passed in directly to search functions.
    credentials : str, optional (default=None)
        OpenML API key.
    """

    def __init__(
        self,
        search_types: Optional[Collection[SearchType]] = None,
        credentials: Optional[str] = None
    ) -> None:

        super().__init__(
            repository_name='openml',
            search_types=search_types
        )

        self.base_url = None

        if not openml.config.apikey:
            openml.config.apikey = credentials

    @staticmethod
    def accepts_user_credentials() -> bool:
        return True

    @classmethod
    @property
    def search_type_options(cls) -> tuple[SearchType, ...]:
        return ('datasets', 'runs', 'tasks', 'evaluations')

    def _get_evaluations_search_output(self) -> pd.DataFrame:
        """Retrieve information on OpenML Evaluations."""
        evaluations_measures = openml.evaluations.list_evaluation_measures()
        evaluations_df = pd.DataFrame()

        # Get evaluation data for each available measure
        for measure in evaluations_measures:
            evaluations_dict = dict(
                openml.evaluations.list_evaluations(measure)
            )

            # Convert string list array_data to list
            if evaluations_dict.get('array_data'):
                try:
                    evaluations_dict['array_data'] = ast.literal_eval(
                        evaluations_dict['array_data']
                    )
                # Occurs if 'array_data' is already a list
                except ValueError:
                    pass

            measure_df = pd.DataFrame(
                map(
                    lambda evaluation: evaluation.__dict__,
                    evaluations_dict.values()
                )
            )

            evaluations_df = pd.concat(
                [evaluations_df, measure_df]
            ).reset_index(drop=True)

        return evaluations_df

    @BaseTypeCollector.validate_search_type
    def get_individual_search_output(
            self,
            search_type: SearchType
    ) -> pd.DataFrame:
        """Queries OpenML API for the specified search type.

        Parameters
        ----------
        search_type : {'datasets', 'runs', 'tasks', 'evaluations'}

        Returns
        -------
        search_df : pandas.DataFrame

        Raises
        ------
        ValueError
            Incorrect search_type provided.
        """

        self.status_queue.put(f'Querying {search_type}...')

        if search_type == 'evaluations':
            return self._get_evaluations_search_output()

        # Use query type to get necessary openml api functions
        base_command = getattr(openml, search_type)
        list_queries = getattr(base_command, f'list_{search_type}')

        index = 0
        size = 10000
        search_df = pd.DataFrame()

        self._update_query_ref(page=index)
        search_results = list_queries(offset=(index * size), size=size)

        while search_results:
            output_df = pd.DataFrame(search_results).transpose()
            output_df['page'] = index + 1
            search_df = pd.concat(
                [search_df, output_df]
            ).reset_index(drop=True)

            index += 1

            self._update_query_ref(page=index)
            search_results = list_queries(offset=(index * size), size=size)

        self.status_queue.put(f'{search_type} search complete.')

        return search_df

    @BaseTypeCollector.validate_search_type
    def get_query_metadata(
            self,
            object_paths: Union[str, Collection[str]],
            search_type: SearchType
    ) -> pd.DataFrame:
        """Retrieves the metadata for the object_paths objects.

        Parameters
        ----------
        object_paths : str or list-like
        search_type : {'datasets', 'runs', 'tasks', 'evaluations'}

        Returns
        -------
        metadata_df : pandas.DataFrame

        Raises
        ------
        ValueError
            Invalid search_type provided.
        """

        object_paths = validate_metadata_parameters(object_paths)

        base_command = getattr(openml, search_type)
        get_query = getattr(base_command, f'get_{search_type[:-1:]}')

        queries = []
        error_queries = []
        for object_path in object_paths:
            self._update_query_ref(object_name=object_path)
            try:
                query = get_query(object_path).__dict__
                queries.append(query)
            except openml.exceptions.OpenMLServerException:
                error_queries.append(object_path)

        metadata_df = pd.DataFrame(queries)

        self.status_queue.put(f' {search_type} metadata query complete.')

        return metadata_df

    def get_all_metadata(
            self,
            search_dict: TypeResultDict
    ) -> TypeResultDict:
        """Retrieves metadata for records contained in input DataFrames.

        Parameters
        ----------
        search_dict : dict
            Dictionary of DataFrames from get_all_search_outputs.

        Returns
        -------
        metadata_dict : dict
        """

        metadata_dict = dict()

        for query, df in search_dict.items():
            self.status_queue.put(f'Querying {query} metadata...')
            if query == 'datasets':
                id_name = 'did'
            elif query == 'runs':
                id_name = 'run_id'
            elif query == 'tasks':
                id_name = 'tid'
            else:
                raise ValueError(
                    f'Query \'{query}\' is not a valid search_type.'
                )

            object_paths = df[id_name].values

            metadata_dict[query] = self.get_query_metadata(
                object_paths=object_paths,
                search_type=query
            )

        self.status_queue.put('Metadata query complete.')

        return metadata_dict
