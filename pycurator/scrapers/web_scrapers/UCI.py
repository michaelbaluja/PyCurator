import os
import re

import pandas as pd
import selenium.webdriver.support.expected_conditions as ec
from selenium.common.exceptions import NoSuchElementException
from flatten_json import flatten
from selenium.webdriver.support.select import By
from selenium.webdriver.support.wait import WebDriverWait

from pycurator.scrapers.base_scrapers import AbstractScraper, AbstractWebScraper
from pycurator.utils import parse_numeric_string, find_first_match


class UCIScraper(AbstractWebScraper):
    """Scrapes the UCI ML Repository for all datasets.

    Parameters
    ----------
    flatten_output : bool, optional (default=False)
        Flag for specifying if nested output should be flattened. Can be passed
        in directly to functions to override set parameter.
    **kwargs : dict, optional
        Allows user to overwrite hardcoded data
    """

    def __init__(
        self,
        flatten_output=False,
        **kwargs
    ):

        super().__init__(
            repository_name='uci',
            flatten_output=flatten_output
        )

        self.base_url = 'https://archive-beta.ics.uci.edu/ml/datasets'

        self.dataset_list_url = f'{self.base_url}?&p%5Boffset%5D=0&p%5Blimit' \
            '%5D=591&p%5BorderBy%5D=NumHits&p%5Border%5D=desc'

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
    def accepts_user_credentials():
        return False

    def run(self, **kwargs):
        """Queries all data from the repository.

        In the following order, this function calls:
        - get_dataset_ids
        - get_all_page_data

        Parameters
        ----------
        **kwargs : dict, optional
            Can temporarily overwrite self attributes.

        Returns
        -------
        dataset_df : pandas.DataFrame
            Return from get_all_page_data(...).
        """

        flatten_output = kwargs.get('flatten_output', self.flatten_output)

        save_dir = kwargs.get('save_dir')
        repo_name = self.repository_name

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

        self.queue.put(f'{self.repository_name} run complete.')
        self.continue_running = False

        return dataset_df

    def _clean_results(self, results):
        """Applies parse functions to relevant entries of the input dictionary.

        For the results of a scraped UCI dataset page, removes text from
        donation or link date, num_citations, and num_views.

        Parameters
        ----------
        results : dict

        Returns
        -------
        results : dict

        Examples
        --------
        >>> self._clean_results({
        ...     'donation_date': 'Donated on 1988-07-01',
        ...     'num_citations': '342 citations',
        ...     'num_views': '29109 views'
        ... })
        {'donation_date': '1988-07-01',
         'num_citations': '342',
         'num_views': 29109}
        """

        donation_date = results.get('donation_date')
        link_date = results.get('link_date')
        num_citations = results.get('num_citations')
        num_views = results.get('num_views')

        if donation_date:
            results['donation_date'] = find_first_match(
                string=donation_date,
                pattern=r'\d+-\d+-\d+'
            )
        if link_date:
            results['link_date'] = find_first_match(
                string=link_date,
                pattern=r'\d+-\d+-\d+'
            )
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
        bool
        """

        return 'Tabular Data Properties' in soup.text

    def is_external(self, soup):
        """Returns if the dataset is externally-hosted.

        Parameters
        ----------
        soup : BeautifulSoup

        Returns
        -------
        bool
        """

        return soup.find('span', string='EXTERNAL')

    @AbstractScraper._pb_indeterminate
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

        self.driver.get(dataset_list_url)
        WebDriverWait(self.driver, 5).until(
            ec.presence_of_element_located((By.CSS_SELECTOR, instance_path))
        )

        soup = self._get_soup(features='html.parser')

        dataset_ids = [
            instance.attrs['href'].split('/')[-1]
            for instance in self.get_variable_tags(
                soup,
                path=instance_path
            )
        ]

        self.queue.put('Dataset id scraping complete.')

        return dataset_ids

    def _get_sibling_tag(
        self,
        soup,
        string,
        pattern=re.compile(r'')
    ):
        """Find Tag from provided specifications, return sibling."""
        try:
            attr = self._get_single_tag_from_tag_info(
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

        Returns
        -------
        result_dict : dict
        """

        result_dict = dict()

        for var, attr in self.individual_attr_dict.items():
            result_dict[var] = self._get_tag_value(
                self.get_single_tag(
                    soup=soup,
                    class_type=re.compile(r''),
                    string=re.compile(attr)
                )
            )

        for var, attr in self.parent_attr_dict.items():
            result_dict[var] = [
                self._get_tag_value(parent_subset)
                for parent in self._get_parent_tag(
                    soup=soup,
                    string=re.compile(attr)
                )
                for parent_subset in parent.find_all(
                    name=re.compile('(p|span)')
                )
            ]

        for var, attr in self.sibling_attr_dict.items():
            result_dict[var] = self._get_tag_value(
                self._get_sibling_tag(soup, attr)
            )

        for var, attr in self.uncle_attr_dict.items():
            tag_texts = [
                self._get_tag_value(
                    tag, separator='~').split('~')
                for tag in self._get_parent_sibling_tags(
                    soup=soup,
                    string=attr
                )
            ]
            if len(tag_texts) == 1:
                tag_texts = tag_texts[0]

            result_dict[var] = tag_texts

        return result_dict

    def get_individual_page_data(
        self,
        url,
        clean=True,
        flatten_output=False,
    ):
        """Returns all data from the requested page.

        Parameters
        ----------
        url : str
        clean : bool, optional (default=True)
        flatten_output : bool, optional (default=False)
            Flag for specifying if nested output should be flattened.

        Returns
        -------
        result_dict : dict
        """

        self.driver.get(url)

        soup = self._get_soup(features='html.parser')
        tag = self._get_sibling_tags(soup, re.compile(r'Abstract'))[0]
        tag_type = tag.name
        tag_path_classes = tag.attrs['class']
        wait_path = '.'.join([tag_type, *tag_path_classes])

        WebDriverWait(self.driver, 5).until_not(
            ec.text_to_be_present_in_element_value(
                (By.CSS_SELECTOR, wait_path),
                'N/A'
            )
        )

        result_dict = self._get_attribute_values(soup)
        result_dict['url'] = self.driver.current_url

        # Get file info
        if not self.is_external(soup):
            try:
                self.driver.find_element_by_link_text('Download').click()
            except NoSuchElementException:
                pass
            else:
                self.driver.switch_to_window(self.driver.window_handles[-1])

                soup = self._get_soup(features='html.parser')
                result_dict['files'] = [
                    self._get_tag_value(child)
                    for child in self.get_variable_tags(
                        soup=soup,
                        path='li > a'
                    )
                ]

                # Close new window and switch back to previous
                self.driver.close()
                self.driver.switch_to_window(self.driver.window_handles[0])

        if clean:
            result_dict = self._clean_results(result_dict)

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
        clean : bool, optional (default=True)
        flatten_output : bool, optional (default=False)
            Flag for specifying if nested output should be flattened.

        Returns
        -------
        dataset_df : pandas.DataFrame
        """

        self.queue.put('Scraping page data...')

        dataset_df = pd.DataFrame()

        for page_id in self._pb_determinate(page_ids):
            url = f'{self.base_url}/{page_id}'

            results = self.get_individual_page_data(
                url=url,
                clean=clean,
                flatten_output=flatten_output
            )
            dataset_df = dataset_df.append(results, ignore_index=True)

        self.queue.put('Scraping complete.')

        return dataset_df