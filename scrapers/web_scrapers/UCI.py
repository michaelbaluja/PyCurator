import os
import re

import pandas as pd
import selenium.webdriver.support.expected_conditions as EC
from flatten_json import flatten
from selenium.webdriver.support.select import By
from selenium.webdriver.support.wait import WebDriverWait
from tqdm import tqdm

from scrapers.base_scrapers import AbstractWebScraper


class UCIScraper(AbstractWebScraper):
    """Scrapes the UCI ML Repository for all datasets.

    Parameters
    ----------
    path_file : str
        Json file for loading path dict.
        Must be of the form {'path_dict': path_dict}
    flatten_output : boolean, optional (default=False)
        Flag for specifying if nested output should be flattened. Can be passed
        in directly to functions to override set parameter.
    **kwargs : dict, optional
        Allows user to overwrite hardcoded data
    """

    def __init__(
        self, 
        path_file,
        flatten_output=False,
        **kwargs
    ):

        super().__init__(
            repository_name='uci', 
            path_file=path_file,
            flatten_output=flatten_output
        )

        try:
            self.structural_paths = self.path_dict['structural_paths']
        except KeyError:
            raise ValueError('Path file must contain structural paths')

        self.base_url = 'https://archive-beta.ics.uci.edu/ml/datasets'

        self.dataset_list_url = f'{self.base_url}?&p%5Boffset%5D=0&p%5Blimit%' \
            '5D=591&p%5BorderBy%5D=NumHits&p%5Border%5D=desc'

        self.instance_path = self.structural_paths['instance_path']
        self.wait_path = self.structural_paths['wait_path']
        self.tabular_wait_path = self.structural_paths['tabular_wait_path']


    @staticmethod
    def accept_user_credentials():
        return False

    def run(self, **kwargs):
        """Queries all data from the repository.

        In the following order, this function calls:
        - get_dataset_ids
        - get_all_page_data

        Parameters
        ----------
        kwargs : dict, optional
            Can temporarily overwrite self attributes.

        Returns
        -------
        dataset_df : DataFrame
            DataFrame returned from get_all_page_data
        """
        
        flatten_output = kwargs.get('flatten_output', self.flatten_output)

        # Set save parameters
        save_dataframe = kwargs.get('save_dataframe')
        repo_name = self.get_repo_name()

        dataset_ids = self.get_dataset_ids(
            dataset_list_url=self.dataset_list_url,
            instance_path=self.instance_path,
        )

        dataset_df = self.get_all_page_data(
            page_ids=dataset_ids,
            flatten_output=flatten_output
        )

        if save_dataframe:
            try:
                save_dir = kwargs['save_dir']
            except KeyError:
                raise ValueError('Must pass save directory to run function.')

            # Ensure save directory exists
            if not os.path.isdir(save_dir):
                os.makedirs(save_dir)

            output_filename = os.path.join(save_dir, f'{repo_name}.json')
            self.save_results(dataset_df, output_filename)

        return dataset_df

    def _clean_results(self, results):
        # Get variables to clean
        donation_date = results.get('donation_date')
        num_citations = results.get('num_citations')
        num_views = results.get('num_views')

        # Remove unnecessary text from temporal/numeric entries
        ## Make sure that the donation date is not null
        if '-' in donation_date:
            results['donation_date'] = re.findall(
                '\d+-\d+-\d+', 
                donation_date
            )[0]
        if num_citations:
            results['num_citations'] = self.parse_numeric(num_citations)[0]      
        if num_views:
            results['num_views'] = self.parse_numeric(num_views)[0]

        return results

    def is_tabular(self, soup):
        """Returns if the dataset related to the given soup page is tabular.

        Parameters
        ----------
        soup : BeautifulSoup

        Returns
        -------
        boolean
        """

        return 'Tabular Data Properties' in soup.text

    def get_dataset_ids(self, dataset_list_url, instance_path):
        """Returns the dataset ids for all datasets on the given page.

        Parameters
        ----------
        dataset_list_url : str
            URL for page containing links to the datasets to scrape.
        instance_path : str
            CSS Selector path for the datasets on the page.

        Returns
        -------
        dataset_ids : list
        """

        # Get the requested url
        self.driver.get(dataset_list_url)
        
        # Wait for instances to load on page
        WebDriverWait(self.driver, 5).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, instance_path))
        )
        
        # Create parsable html object
        soup = self._get_soup(features='html.parser')
        
        # Gather the instances and parse the ids
        dataset_ids = [
            instance.attrs['href'].split('/')[-1] 
            for instance in soup.select(instance_path)
        ]
        
        return dataset_ids

    def get_individual_page_data(
        self,
        url, 
        clean=True,
        flatten_output=False,
        **kwargs
    ):
        """Returns all data from the requested page.
        
        Parameters
        ----------
        url : str
        clean : boolean, optional (default=True)
        flatten_output : boolean, optional (default=False)
            Flag for specifying if nested output should be flattened.
        kwargs : dict, optional (default=None)
            Contains attribute dicts to use.
        
        Returns
        -------
        result_dict : dict
        """
        
        single_attribute_paths = self.path_dict.get(
            'single_attribute_paths', 
            dict()
        )
        variable_attribute_paths = self.path_dict.get(
            'variable_attribute_paths', 
            dict()
        )
        tabular_attribute_paths = self.path_dict.get(
            'tabular_attribute_paths', 
            None
        )
        
        # Get the requested url
        self.driver.get(url)
        
        # Wait for pertinent sections to load
        try:
            WebDriverWait(self.driver, 5).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, self.wait_path))
            )
        except:
            # This occurs when a dataset listed is "External". The difference
            # in page layout (which appears to be caused by the lack of author
            # information) causes the above code to raise an error as the 
            # wait path cannot be found.
            return None
        
        # Extract and convert html data
        soup = self._get_soup(features='html.parser')

        # Add tabular info
        if self.is_tabular(soup) and tabular_attribute_paths:
            # Wait to ensure that tabular properties loaded
            WebDriverWait(self.driver, 5).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, self.tabular_wait_path)
                )
            )

            # Extract and convert html data
            soup = self._get_soup(features='html.parser')

            try:
                single_attribute_paths = {
                    **single_attribute_paths, 
                    **tabular_attribute_paths
                }
            except NameError:
                single_attribute_paths = tabular_attribute_paths
        
        # Retrieve attribute values from parsed html
        single_values = {
            attribute: self.get_single_attribute_value(soup=soup, path=path) 
            for attribute, path in single_attribute_paths.items()
        }
        variable_values = {
            attribute: self.get_variable_attribute_values(soup, path)
            for attribute, path in variable_attribute_paths.items()
        }
        
        # Add results
        if single_values and variable_values:
            result_dict = {**single_values, **variable_values}
        elif single_values:
            result_dict = single_values
        elif variable_values:
            result_dict = variable_values
        else:
            result_dict = dict()

        # Get file info 
        self.driver.find_element_by_link_text('Download').click()
        
        soup = self._get_soup(features='html.parser')

        if '404' not in soup.text:
            result_dict['links'] = self.get_variable_attribute_values(
                soup, 
                'body > ul > li'
            )
        
        # Clean results (if instructed)
        if clean:
            result_dict = self._clean_results(result_dict)
        
        # Flatten output (if instructed)
        if flatten_output:
            result_dict = flatten(result_dict)
        
        return result_dict

    def get_all_page_data(
        self,
        page_ids,
        clean=True,
        flatten_output=False
    ):
        """Returns data for all pages for the requested base url.
        
        Parameters
        ----------
        page_ids : list-like
            dataset ids to use for pulling up each page.
        clean : boolean, optional (default=True)
        flatten_output : boolean, optional (default=False)
            Flag for specifying if nested output should be flattened.
            
        Returns
        -------
        dataset_df : DataFrame
        """
        
        # Create hollow output dataframe
        dataset_df = pd.DataFrame()
        
        # Loop for each dataset page
        for page_id in tqdm(page_ids):
            url = f'{self.base_url}/{page_id}'
            
            # Retrieve and clean results
            results = self.get_individual_page_data(
                url=url, 
                clean=clean,
                flatten_output=flatten_output
            )
            # Add results to total result dataframe
            dataset_df = dataset_df.append(results, ignore_index=True)
        
        # Remove unnecessary nested columns
        #   Datasets that don't have nested data will force the DataFrame to 
        #   keep the nested column names
        if flatten_output:
            columns_to_drop = self.path_dict.get(
                'variable_attribute_paths', 
                dict()
            ).keys()
            dataset_df = dataset_df.drop(columns=columns_to_drop)
        
        return dataset_df
