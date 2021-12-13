from collections import OrderedDict

import openml
import pandas as pd
from selenium.webdriver.remote.errorhandler import InvalidArgumentException
from tqdm import tqdm

from scrapers.base_scrapers import AbstractTypeScraper, AbstractWebScraper
from utils import flatten_nested_df


class OpenMLScraper(AbstractTypeScraper, AbstractWebScraper):
    """Scrapes the OpenML API for all data relating to the given search types.

    Parameters
    ----------
    path_file : str
        Json file for loading path dict.
        Must be of the form {search_type: {path_type: path_dict}}
    search_types : list-like, optional
        Types to search over. Can be (re)set via set_search_types() or passed in
        directly to search functions.
    flatten_output : boolean, optional (default=None)
        Flag for specifying if nested output should be flattened. Can be passed
        in directly to functions to override set parameter.
    credentials : str, optional
        API token or pkl filepath containing credentials in dict.
        If filepath, data in file must be formatted as a dictionary of the form
        data_dict['{REPO_NAME}_TOKEN']: MY_KEY, or as a string containing the 
        key.
    """

    search_type_options = ('datasets', 'runs', 'tasks', 'evaluations')

    def __init__(
        self,
        path_file,
        search_types=None,
        flatten_output=None,
        credentials=None
    ):

        # Initialize parent classes
        AbstractTypeScraper.__init__(
            self,
            repository_name='openml',
            search_types=search_types,
        )

        AbstractWebScraper.__init__(
            self,
            repository_name='openml',
            path_file=path_file,
            flatten_output=flatten_output
        )

        self.base_url = None

        if not openml.config.apikey:
            openml.config.apikey = credentials

    @staticmethod
    def accept_user_credentials():
        return True

    def _get_value_attributes(self, obj):
        """
        Given an object, returns a list of the object's value-based variables

        Parameters
        ----------
        obj : list-like 
            object to be analyzed 

        Returns
        -------
        attributes : list
            value-based variables for the object given
        """

        # This code will pull all of the attributes of the provided class that
        # are not callable or "private" for the class.
        return [
            attr for attr in dir(obj)
            if not hasattr(getattr(obj, attr), '__call__')
            and not attr.startswith('_')
        ]

    def _get_evaluations_search_output(self, flatten_output):
        # Get different evaluation measures we can search for
        evaluations_measures = openml.evaluations.list_evaluation_measures()

        # Create DataFrame to store attributes
        evaluations_df = pd.DataFrame()

        # Get evaluation data for each available measure
        for measure in tqdm(evaluations_measures):
            # Query all data for a given evaluation measure
            evaluations_dict = openml.evaluations.list_evaluations(measure)

            try:
                # Grab one of the evaluations in order to extract attributes
                sample_evaluation = next(iter(evaluations_dict.items()))[1]
            # StopIteration will occur in the preceding code if an evaluation
            # search returns no results for a given measure
            except StopIteration:
                continue

            # Get list of attributes the evaluation offers
            evaluations_attributes = self._get_value_attributes(
                sample_evaluation)

            # Adds the queried data to the DataFrame
            for query in evaluations_dict.values():
                attribute_dict = {
                    attribute: getattr(query, attribute)
                    for attribute in evaluations_attributes
                }
                evaluations_df = evaluations_df.append(
                    attribute_dict,
                    ignore_index=True
                )

            evaluations_df = flatten_nested_df(evaluations_df)

        return evaluations_df

    def get_dataset_related_tasks(self, df):
        """Queries the task/run information related to the provided datasets.

        Parameters
        ----------
        data_df : DataFrame

        Returns
        -------
        data_df : DataFrame
            Original input with task and run information appended.
        """

        err_msg = 'Sorry, this data set does not seem to exist (anymore).'
        search_df = pd.DataFrame()
        urls = df['openml_url'].dropna()
        for url in tqdm(urls):
            # Create aggregate containers
            num_runs_list = []
            task_type_list = []
            task_id_list = []
            object_dict = dict()

            # Get page
            try:
                self.driver.get(url)
            except InvalidArgumentException as e:
                print(url)
                raise e
            soup = self._get_soup(features='html.parser')

            # If the page doesn't exist anymore, skip to the next loop
            if err_msg in soup.text:
                continue

            # Add url to dict (after ensuring functionality) for merging back
            object_dict['openml_url'] = url

            # Add download info to dict
            downloads = self.get_single_attribute_value(
                soup=soup,
                path=self.path_dict['downloads']
            )
            downloads = self.parse_numeric(downloads)
            object_dict['num_downloads'], object_dict['num_unique_downloads'] = downloads

            # Get number of tasks
            num_tasks = self.get_single_attribute_value(
                soup=soup,
                path=self.path_dict['num_tasks']
            )
            num_tasks = int(self.parse_numeric(num_tasks)[0])

            # Get task info
            for task_idx in range(num_tasks):
                path = self.path_dict['task']
                path = f'{path}({9 + task_idx})'

                try:
                    # Get task object
                    task = self._get_single_attribute(soup, path)
                    task_link = task.a

                    # Extract task type
                    task_type = task_link.text.split(' on')[0]
                    task_type_list.append(task_type)

                    # Extract task id
                    task_id = task_link.attrs['href'][2:]
                    task_id_list.append(task_id)

                    # Extract num runs per task
                    num_runs = task.b.text
                    num_runs = int(self.parse_numeric(num_runs)[0])
                    num_runs_list.append(num_runs)
                except:
                    print(f'{url}\n{path}')

            # Add data to cumulative df
            object_dict['num_runs'] = sum(num_runs_list)
            object_dict['num_tasks'] = num_tasks
            object_dict['task_types'] = task_type_list
            object_dict['task_ids'] = task_id_list

            search_df = search_df.append(object_dict, ignore_index=True)

        return search_df

    def get_individual_search_output(self, search_type, **kwargs):
        """Returns information about all queried information types on OpenML.

        Parameters
        ----------
        search_type : str
        kwargs : dict, optional
            Can temporarily overwrite self flatten_output argument.

        Returns
        -------
        search_df : DataFrame
        """

        flatten_output = kwargs.get('flatten_output', self.flatten_output)

        # Ensure parameters are valid
        if search_type not in OpenMLScraper.search_type_options:
            raise ValueError(f'"{search_type}" is not a valid object type')

        # Handle special case for evaluations
        if search_type == 'evaluations':
            return self._get_evaluations_search_output(flatten_output)

        # Use query type to get necessary openml api functions
        base_command = getattr(openml, search_type)
        list_queries = getattr(base_command, f'list_{search_type}')

        # Get base info on every object listed on OpenML for the given query
        # Since there's too many runs to get all at once, we need to search
        # with offsets and rest periods so the server doesn't overload.

        # Set search params
        index = 0
        size = 10000
        search_df = pd.DataFrame()

        # Perform initial search
        self._print_progress(index)
        search_results = list_queries(offset=(index * size), size=size)

        # Serach until all queries have been returned
        while search_results:
            # Add results to cumulative output df
            output_df = pd.DataFrame(search_results).transpose()
            output_df['page'] = index + 1
            search_df = pd.concat([search_df, output_df]
                                  ).reset_index(drop=True)

            # Increment search range
            index += 1

            # Perform next search
            self._print_progress(index)
            search_results = list_queries(offset=(index * size), size=size)

        # Flatten output (if necessary)
        if flatten_output:
            search_df = flatten_nested_df(search_df)

        return search_df

    def get_query_metadata(self, object_paths, search_type, **kwargs):
        """Retrieves the metadata for the file/files listed in object_paths

        Parameters
        ----------
        object_paths : str/list-like
        search_type : str
        kwargs : dict, optional
            Can temporarily overwrite self flatten_output argument

        Returns
        -------
        metadata_df : DataFrame
        """

        flatten_output = kwargs.get('flatten_output', self.flatten_output)

        # Ensure object paths are of the proper form
        object_paths = self.validate_metadata_parameters(object_paths)

        base_command = getattr(openml, search_type)
        get_query = getattr(base_command, f'get_{search_type[:-1:]}')

        # Request each query
        queries = []
        error_queries = []
        for object_path in tqdm(object_paths):
            try:
                queries.append(get_query(object_path))
            except:
                error_queries.append(object_path)

        # Get list of metadata attributes the queries offer
        query_attributes = self._get_value_attributes(queries[0])

        # Create DataFrame to store metadata attributes
        metadata_df = pd.DataFrame(columns=query_attributes)

        # Append attributes of each dataset to the DataFrame
        for query in queries:
            attribute_dict = {
                attribute: getattr(query, attribute)
                for attribute in query_attributes
            }
            metadata_df = metadata_df.append(attribute_dict, ignore_index=True)

        # Flatten the nested DataFrame
        if flatten_output:
            metadata_df = flatten_nested_df(metadata_df)

        if search_type == 'datasets':
            web_df = self.get_dataset_related_tasks(metadata_df)
            metadata_df = pd.merge(metadata_df, web_df, on='openml_url')

        return metadata_df

    def get_all_metadata(self, search_dict, **kwargs):
        """Retrieves all metadata that relates to the provided DataFrames.

        Parameters
        ----------
        search_dict : dict
            Dictionary of DataFrames from get_all_search_outputs.
        kwargs : dict, optional
            Can temporarily overwrite self flatten_output argument.

        Returns
        -------
        metadata_dict : OrderedDict
            OrderedDict of DataFrames with metadata for each query.
            Order matches the order of search_dict.
        """

        metadata_dict = OrderedDict()

        for query, df in search_dict.items():
            print(f'Querying {query} metadata.')
            if query == 'datasets':
                id_name = 'did'
            elif query == 'runs':
                id_name = 'run_id'
            elif query == 'tasks':
                id_name = 'tid'

            # Grab the object paths as the id's from the DataFrame
            object_paths = df[id_name].values

            metadata_dict[query] = self.get_query_metadata(
                object_paths=object_paths,
                search_type=query,
                **kwargs
            )

        return metadata_dict
