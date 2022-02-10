import re

import openml
import pandas as pd
from selenium.webdriver.remote.errorhandler import InvalidArgumentException

from scrapers.base_scrapers import AbstractTypeScraper, AbstractWebScraper
from utils import flatten_nested_df, parse_numeric_string


class OpenMLScraper(AbstractTypeScraper, AbstractWebScraper):
    """Scrapes the OpenML API for all data relating to the given search types.

    Parameters
    ----------
    path_file : str, optional (default=None)
        Json file for loading path dict.
        Must be of the form {search_type: {path_type: path_dict}}
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

    Notes
    -----
    The method of web scraping has changed for this class, rendering the
    presence of a path_file unnecessary. The class initialization will change
    at a later point to reflect this.
    """

    def __init__(
        self,
        path_file=None,
        scrape=True,
        search_types=None,
        flatten_output=None,
        credentials=None
    ):

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
                path_file=path_file,
            )

            self.attr_dict = {
                'downloads': r'downloaded by \d+ people',
                'num_tasks': r'\d+ tasks'
            }

        self.base_url = None

        if not openml.config.apikey:
            openml.config.apikey = credentials

    @staticmethod
    def accept_user_credentials():
        return True

    @classmethod
    def get_search_type_options(cls):
        return ('datasets', 'runs', 'tasks', 'evaluations')

    def _get_value_attributes(self, obj):
        """Returns a list of an object's value-based variables.

        Given an object, returns the attributes that are not callable or
        contain a leading underscore.

        Parameters
        ----------
        obj

        Returns
        -------
        list
            Value-based variables for the object given.
        """

        return [
            attr for attr in dir(obj)
            if not hasattr(getattr(obj, attr), '__call__')
            and not attr.startswith('_')
        ]

    def _get_evaluations_search_output(self, flatten_output):
        evaluations_measures = openml.evaluations.list_evaluation_measures()

        evaluations_df = pd.DataFrame()

        # Get evaluation data for each available measure
        for measure in evaluations_measures:
            evaluations_dict = openml.evaluations.list_evaluations(measure)

            try:
                # Grab one of the evaluations in order to extract attributes
                sample_evaluation = next(iter(evaluations_dict.items()))[1]
            except StopIteration:
                # Occurs if no results are available for a given measure
                continue

            # Get list of attributes the evaluation offers
            evaluations_attributes = self._get_value_attributes(
                sample_evaluation
            )

            for query in evaluations_dict.values():
                attribute_dict = {
                    attribute: getattr(query, attribute)
                    for attribute in evaluations_attributes
                }
                evaluations_df = evaluations_df.append(
                    attribute_dict,
                    ignore_index=True
                )

            if flatten_output:
                evaluations_df = flatten_nested_df(evaluations_df)

        return evaluations_df

    def get_dataset_related_tasks(self, df):
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

            downloads = self.get_single_tag(
                soup=soup,
                string=re.compile(self.attr_dict['downloads'])
            )
            downloads = parse_numeric_string(downloads)
            object_dict['num_downloads'], \
                object_dict['num_unique_downloads'] = downloads

            num_tasks = self._get_tag_value(
                self._get_parent_tag(
                    soup=soup,
                    string=re.compile(self.attr_dict['num_tasks'])
                )
            )
            num_tasks = parse_numeric_string(num_tasks, cast=True)

            task_tags = self._get_parent_sibling_tags(
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

    def get_individual_search_output(self, search_type, **kwargs):
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
        search_type_options = OpenMLScraper.get_search_type_options()

        if search_type not in search_type_options:
            raise ValueError(f'Can only search {search_type_options}.')

        self.queue.put(f'Querying {search_type}...')

        if search_type == 'evaluations':
            return self._get_evaluations_search_output(flatten_output)

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

    def get_query_metadata(self, object_paths, search_type, **kwargs):
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

        search_type_options = self.get_search_type_options()
        if search_type not in search_type_options:
            raise ValueError(f'Can only search {search_type_options}.')

        base_command = getattr(openml, search_type)
        get_query = getattr(base_command, f'get_{search_type[:-1:]}')

        queries = []
        error_queries = []
        for object_path in object_paths:
            self._update_query_ref(object_name=object_path)
            try:
                queries.append(get_query(object_path))
            except TypeError:
                error_queries.append(object_path)

        query_attributes = self._get_value_attributes(queries[0])

        metadata_df = pd.DataFrame(columns=query_attributes)

        for query in queries:
            attribute_dict = {
                attribute: getattr(query, attribute)
                for attribute in query_attributes
            }
            metadata_df = metadata_df.append(attribute_dict, ignore_index=True)

        if flatten_output:
            metadata_df = flatten_nested_df(metadata_df)

        if search_type == 'datasets' and self.scrape:
            web_df = self.get_dataset_related_tasks(metadata_df)
            metadata_df = pd.merge(metadata_df, web_df, on='openml_url')

        self.queue.put(f' {search_type} metadata query complete.')

        return metadata_df

    def get_all_metadata(self, search_dict, **kwargs):
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
                raise ValueError(f'Query \'{query}\' is not a valid search_type.')

            object_paths = df[id_name].values

            metadata_dict[query] = self.get_query_metadata(
                object_paths=object_paths,
                search_type=query,
                **kwargs
            )

        self.queue.put('Metadata query complete.')

        return metadata_dict
