import ast
import re
from collections.abc import Collection
from typing import Any, Optional, Union

import openml
import pandas as pd
from flatten_json import flatten
from selenium.webdriver.remote.errorhandler import InvalidArgumentException

from pycurator.scrapers.base_scrapers import (
    AbstractTypeScraper,
    AbstractWebScraper
)
from pycurator.utils import flatten_nested_df, parse_numeric_string, web_utils
from pycurator.utils.typing import (
    SearchType,
    TypeResultDict
)


class OpenMLScraper(AbstractTypeScraper, AbstractWebScraper):
    """Scrapes the OpenML API for all data relating to the given search types.

    Parameters
    ----------
    scrape : bool, optional (default=True)
        Flag for requesting web scraping as a method for additional metadata
        collection.
    search_types : list-like, optional
        Types to search over. Can be (re)set via set_search_types() or passed
        in directly to search functions.
    flatten_output : bool, optional (default=None)
        Flag for specifying if nested output should be flattened. Can be passed
        in directly to functions to override set parameter.
    credentials : str, optional (default=None)
        JSON filepath containing credentials in form {repository_name}: 'key'.
    """

    def __init__(
        self,
        scrape: bool = True,
        search_types: Optional[Collection[SearchType]] = None,
        flatten_output: Optional[bool] = None,
        credentials: Optional[str] = None
    ) -> None:

        self.scrape = scrape

        AbstractTypeScraper.__init__(
            self,
            repository_name='openml',
            search_types=search_types,
            flatten_output=flatten_output
        )

        if self.scrape:
            AbstractWebScraper.__init__(
                self,
                repository_name='openml',
            )

            self.attr_dict = {
                'downloads': r'downloaded by \d+ people',
                'num_tasks': r'\d+ tasks'
            }

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

    def _get_evaluations_search_output(self, **kwargs: Any) -> pd.DataFrame:
        flatten_output = kwargs.get('flatten_output', self.flatten_output)

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

            if flatten_output:
                evaluations_dict = flatten(evaluations_dict)

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

    def get_dataset_related_tasks(self, df: pd.DataFrame) -> pd.DataFrame:
        """Queries the task/run information related to the provided datasets.

        Parameters
        ----------
        df : pandas.DataFrame

        Returns
        -------
        data_df : pandas.DataFrame
            Original input with task and run information appended.
        """

        err_msg = 'Sorry, this data set does not seem to exist (anymore).'
        search_df = pd.DataFrame()
        urls = df['openml_url'].dropna()

        self.queue.put('Scraping dataset task/run information...')

        for url in urls:
            self._update_query_ref(page=url)

            num_runs_list = []
            task_type_list = []
            task_id_list = []
            object_dict = dict()

            try:
                self.driver.get(url)
            except InvalidArgumentException:
                self.queue.put(f'Invalid URL provided for scraping: {url}')
            soup = self._get_soup(features='html.parser')

            if err_msg in soup.text:
                continue

            object_dict['openml_url'] = url

            downloads = web_utils.get_tag_value(
                web_utils.get_single_tag(
                    soup=soup,
                    string=re.compile(self.attr_dict['downloads'])
                )
            )
            downloads = parse_numeric_string(downloads)
            object_dict['num_downloads'], \
                object_dict['num_unique_downloads'] = downloads

            num_tasks = web_utils.get_tag_value(
                web_utils.get_parent_tag(
                    soup=soup,
                    string=re.compile(self.attr_dict['num_tasks'])
                )
            )
            num_tasks = parse_numeric_string(num_tasks, cast=True)

            task_tags = web_utils.get_parent_sibling_tags(
                soup=soup,
                string=re.compile(self.attr_dict['num_tasks']),
                limit=num_tasks
            )

            for task in task_tags:
                try:
                    task_link = task.a

                    task_type = task_link.text.split(' on')[0]
                    task_type_list.append(task_type)

                    task_id = task_link.attrs['href'][2:]
                    task_id_list.append(task_id)

                    num_runs = task.b.text
                    num_runs = parse_numeric_string(num_runs, cast=True)
                    num_runs_list.append(num_runs)
                except AttributeError:
                    pass

            # Add data to cumulative df
            object_dict['num_runs'] = sum(num_runs_list)
            object_dict['num_tasks'] = num_tasks
            object_dict['task_types'] = task_type_list
            object_dict['task_ids'] = task_id_list

            search_df = search_df.append(object_dict, ignore_index=True)

        self.queue.put('Dataset info scraping complete.')

        return search_df

    def get_individual_search_output(
            self,
            search_type: SearchType,
            **kwargs: Any
    ) -> pd.DataFrame:
        """Returns information about all queried information types on OpenML.

        Parameters
        ----------
        search_type : {'datasets', 'runs', 'tasks', 'evaluations'}
        **kwargs : dict, optional
            Can temporarily overwrite self flatten_output argument.

        Returns
        -------
        search_df : pandas.DataFrame

        Raises
        ------
        ValueError
            Incorrect search_type provided.
        """

        flatten_output = kwargs.get('flatten_output', self.flatten_output)
        search_type_options = self.search_type_options

        if search_type not in search_type_options:
            raise ValueError(f'Can only search {search_type_options}.')

        self.queue.put(f'Querying {search_type}...')

        if search_type == 'evaluations':
            return self._get_evaluations_search_output(
                flatten_output=flatten_output
            )

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

        if flatten_output:
            search_df = flatten_nested_df(search_df)

        self.queue.put(f'{search_type} search complete.')

        return search_df

    def get_query_metadata(
            self,
            object_paths: Union[str, Collection[str]],
            search_type: SearchType,
            **kwargs: Any
    ) -> pd.DataFrame:
        """Retrieves the metadata for the file/files listed in object_paths.

        Parameters
        ----------
        object_paths : str or list-like
        search_type : {'datasets', 'runs', 'tasks', 'evaluations'}
        **kwargs : dict, optional
            Can temporarily overwrite self flatten_output argument.

        Returns
        -------
        metadata_df : pandas.DataFrame

        Raises
        ------
        ValueError
            Invalid search_type provided.
        """

        flatten_output = kwargs.get('flatten_output', self.flatten_output)
        object_paths = self.validate_metadata_parameters(object_paths)

        search_type_options = self.search_type_options
        if search_type not in search_type_options:
            raise ValueError(f'Can only search {search_type_options}.')

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

        if flatten_output:
            metadata_df = flatten_nested_df(metadata_df)

        if search_type == 'datasets' and self.scrape:
            web_df = self.get_dataset_related_tasks(metadata_df)
            metadata_df = pd.merge(metadata_df, web_df, on='openml_url')

        self.queue.put(f' {search_type} metadata query complete.')

        return metadata_df

    def get_all_metadata(
            self,
            search_dict: TypeResultDict,
            **kwargs: Any
    ) -> TypeResultDict:
        """Retrieves all metadata that relates to the provided DataFrames.

        Parameters
        ----------
        search_dict : dict
            Dictionary of DataFrames from get_all_search_outputs.
        **kwargs : dict, optional
            Can temporarily overwrite self flatten_output argument.

        Returns
        -------
        metadata_dict : dict
        """

        metadata_dict = dict()

        for query, df in search_dict.items():
            self.queue.put(f'Querying {query} metadata...')
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
                search_type=query,
                **kwargs
            )

        self.queue.put('Metadata query complete.')

        return metadata_dict
