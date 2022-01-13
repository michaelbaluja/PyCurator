import os
import re

import pandas as pd
import selenium.webdriver.support.expected_conditions as EC
from flatten_json import flatten
from selenium.webdriver.support.select import By
from selenium.webdriver.support.wait import WebDriverWait
from tqdm import tqdm

from scrapers.base_scrapers import AbstractWebScraper
from utils import parse_numeric_string


class UCIScraper(AbstractWebScraper):
    """Scrapes the UCI ML Repository for all datasets.

    Parameters
    ----------
    path_file : str, optional (default=None)
        Json file for loading path dict.
        Must be of the form {'path_dict': path_dict}
    flatten_output : boolean, optional (default=False)
        Flag for specifying if nested output should be flattened. Can be passed
        in directly to functions to override set parameter.
    **kwargs : dict, optional
        Allows user to overwrite hardcoded data

    Notes
    -----
    The method of webscraping has changed for this class, rendering the presence
    of a path_file unnecessary. The class initialization will change at a later
    point to reflect this.
    """

    def __init__(
        self, 
        path_file=None,
        flatten_output=False,
        **kwargs
    ):

        super().__init__(
            repository_name='uci', 
            path_file=path_file,
            flatten_output=flatten_output
        )

        self.base_url = 'https://archive-beta.ics.uci.edu/ml/datasets'

        self.dataset_list_url = f'{self.base_url}?&p%5Boffset%5D=0&p%5Blimit%' \
            '5D=591&p%5BorderBy%5D=NumHits&p%5Border%5D=desc'

        # Set scrape attribute dicts
        self.parent_attr_dict = {
            'keywords': r'Keywords',
            'license': r'License'
        }
        self.sibling_attr_dict = {
            'abstract': r'Abstract',
            'dataset_characteristics': r'Dataset Characteristics',
            'associated_tasks': r'Associated Tasks',
            'num_instances': r'# of Instances',
            'subject_area': r'Subject Area',
            'doi': r'doi',
            'creation_purpose': r'For what purpose was the dataset created?',
            'funders': r'Who funded the creation of the dataset?',
            'instances_represent': r'What do the instances that comprise the '
                r'dataset represent?',
            'recommended_data_split': r'Are there recommended data splits?',
            'sensitive_data': r'Does the dataset contain data that might be'
                r' considered sensitive in any way?',
            'preprocessing_done': r'Was there any data preprocessing '
                r'performed?',
            'previous_tasks': r'Has the dataset been used for any tasks '
                r'already?',
            'additional_info': r'Additional Information',
            'citation_requests/acknowledgements': r'Citation Requests'
                r'/Acknowledgements',
            'missing_values': r'Does this dataset contain missing values?',
            'missing_value_placeholder': r'What symbol is used to indicate '
                r'missing data?',
            'num_attributes': r'Number of Attributes'
        }
        self.uncle_attr_dict = {
            'creators': r'Creators'
        }
        self.individual_attr_dict = {
            'donation_date': r'Donated on',
            'link_date': r'Linked on',
            'num_views': r'\d+ views',
            'num_citations': r'\d+ citations'
        }


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
        save_dir = kwargs.get('save_dir')
        repo_name = self.get_repo_name()

        dataset_ids = self.get_dataset_ids(
            dataset_list_url=self.dataset_list_url,
            instance_path='p > a',
        )

        dataset_df = self.get_all_page_data(
            page_ids=dataset_ids,
            flatten_output=flatten_output
        )
        
        if save_dir:
            output_filename = os.path.join(save_dir, f'{repo_name}.json')
            self.queue.put(f'Saving output to "{output_filename}.')
            self.save_results(dataset_df, output_filename)
            self.queue.put('Save complete.')

        return dataset_df

    def _clean_results(self, results):
        # Get variables to clean
        donation_date = results.get('donation_date')
        link_date = results.get('link_date')
        num_citations = results.get('num_citations')
        num_views = results.get('num_views')

        # Remove unnecessary text from temporal/numeric entries
        ## Make sure that the donation date is not null
        if donation_date:
            try:
                results['donation_date'] = re.findall(
                    '\d+-\d+-\d+', 
                    donation_date
                )[0]
            # Error occurs when no date is present
            except IndexError:
                results['donation_date'] = None
        if link_date:
            results['link_date'] = re.findall(
                '\d+-\d+-\d+', 
                link_date
            )[0]
        if num_citations:
            results['num_citations'] = parse_numeric_string(num_citations)
        if num_views:
            results['num_views'] = parse_numeric_string(num_views)

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

    def is_external(self, soup):
        """Returns if the dataset is externally-hosted.

        Parameters
        ----------
        soup : BeautifulSoup

        Returns
        -------
        boolean
        """

        return soup.find('span', string='EXTERNAL')

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

        self.queue.put('Scraping dataset ids...')

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

        self.queue.put('Dataset id scraping complete.')
        
        return dataset_ids
    
    def _get_sibling_attribute(
        self,
        soup, 
        string,
        pattern=re.compile(r'')
    ):
        """Find Tag from provided specifications, return sibling."""
        try:
            attr = self._get_single_attribute_from_tag_info(
                soup=soup, 
                class_type=pattern,
                string=string,
            ).next_sibling
        except AttributeError:
            attr = None

        return attr

    def _get_attribute_values(self, soup):
        """Scrapes the possible attributes provided in the repository

        Parameters
        ----------
        soup : BeautifulSoup
            BeautifulSoup object containing the html to be parsed.

        Returns
        -------
        result_dict : dict
        """

        result_dict = dict()
        
        # Get date (donation/linked), views, citattions
        for var, attr in self.individual_attr_dict.items():
            result_dict[var] = self._get_attribute_value(
                self.get_single_attribute(
                    soup=soup, 
                    class_type=re.compile(r''),
                    string=re.compile(attr)
                )
            )

        # Get keywords/license
        for var, attr in self.parent_attr_dict.items():
            result_dict[var] = [
                self._get_attribute_value(parent_subset)
                for parent in self._get_parent_attribute(
                    soup=soup, 
                    string=re.compile(attr)
                )
                for parent_subset in parent.find_all(name=re.compile('(p|span)'))
            ]

        # Get General Information
        for var, attr in self.sibling_attr_dict.items():
            result_dict[var] = self._get_attribute_value(
                self._get_sibling_attribute(soup, attr)
            )

        # Get creators
        for var, attr in self.uncle_attr_dict.items():
            try:
                tag_texts = [
                    self._get_attribute_value(pibling, separator='~').split('~')
                    for pibling in self._get_pibling_attributes(
                        soup=soup,
                        string=r'Creators'
                    )
                ]
                if len(tag_texts) == 1:
                    tag_texts = tag_texts[0]

                result_dict[var] = tag_texts
            except:
                pass

        return result_dict
    

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
        
        # Get the requested url
        self.driver.get(url)
        
        # Wait for pertinent sections to load
        WebDriverWait(self.driver, 5).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'td > p'))
        )
        
        # Extract and convert html data
        soup = self._get_soup(features='html.parser')
        
        # Retrieve attribute values from parsed html
        result_dict = self._get_attribute_values(soup)
        result_dict['url'] = self.driver.current_url

        # Get file info 
        if not self.is_external(soup):
            ## Open file tab and switch to it
            self.driver.find_element_by_link_text('Download').click()
            self.driver.switch_to_window(self.driver.window_handles[-1])        

            soup = self._get_soup(features='html.parser')
            result_dict['files'] = [
                self._get_attribute_value(child)
                for child in self.get_variable_attribute(
                    soup=soup, 
                    path='li > a'
                )
            ]

            ## Close new window and switch back to previous
            self.driver.close()
            self.driver.switch_to_window(self.driver.window_handles[0])
        
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
        
        self.queue.put('Scraping page data...')

        # Create output dataframe
        dataset_df = pd.DataFrame()
        
        # Loop for each dataset page
        for page_id in tqdm(page_ids[:10]):
            url = f'{self.base_url}/{page_id}'
            
            # Retrieve and clean results
            results = self.get_individual_page_data(
                url=url, 
                clean=clean,
                flatten_output=flatten_output
            )
            # Add results to total result dataframe
            dataset_df = dataset_df.append(results, ignore_index=True)
        
        self.queue.put('Scraping complete.')

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
