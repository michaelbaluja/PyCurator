import re
from collections.abc import Collection
from typing import Any

import bs4
import pandas as pd
import selenium.webdriver.support.expected_conditions as ec
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.support.select import By
from selenium.webdriver.support.wait import WebDriverWait

from pycurator._typing import AttributeDict
from pycurator.collectors import (
    BaseCollector,
    BaseWebCollector
)
from pycurator.utils import (
    find_first_match,
    parse_numeric_string,
    save_results
)
from pycurator.utils import web_utils


class UCIScraper(BaseWebCollector):
    """UCI Machine Learning Repository web scraper.

    Parameters
    ----------
    **kwargs : dict, optional
        Allows user to overwrite hardcoded data
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(repository_name='uci')

        self.base_url = kwargs.get(
            'base_url',
            'https://archive-beta.ics.uci.edu/ml/datasets'
        )

        self.dataset_list_url = kwargs.get(
            'dataset_list_url',
            f'{self.base_url}?&p%5Boffset%5D=0&p%5Blimit%5D=591&p%5BorderBy%5D=NumHits&p%5Border%5D=desc'  # noqa E501
        )

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
            'doi': r'DOI',
            'creation_purpose': r'For what purpose was the dataset created?',
            'funders': r'Who funded the creation of the dataset?',
            'instances_represent': r'What do the instances that comprise the dataset represent?',  # noqa E501
            'recommended_data_split': r'Are there recommended data splits?',
            'sensitive_data': r'Does the dataset contain data that might be considered sensitive in any way?',  # noqa E501
            'preprocessing_done': r'Was there any data preprocessing performed?',  # noqa E501
            'previous_tasks': r'Has the dataset been used for any tasks already?',  # noqa E501
            'additional_info': r'Additional Information',
            'citation_requests/acknowledgements': r'Citation Requests/Acknowledgements',  # noqa E501
            'missing_values': r'Does this dataset contain missing values?',
            'missing_value_placeholder': r'What symbol is used to indicate missing data?',  # noqa E501
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

        self.wait_path_strings = {
            'keywords': self.parent_attr_dict['keywords'],
            'license': self.parent_attr_dict['license'],
            'general': self.sibling_attr_dict['abstract'],
            'descriptive': self.sibling_attr_dict['creation_purpose'],
            'creators': self.uncle_attr_dict['creators'],
            'headers': self.individual_attr_dict['num_views']
        }

    @staticmethod
    def accepts_user_credentials() -> bool:
        return False

    def run(self, **kwargs: Any) -> None:
        """Queries all data from the repository.

        In the following order, this function calls:
        - get_dataset_ids
        - get_all_page_data

        Parameters
        ----------
        **kwargs : dict, optional
            Can temporarily overwrite self attributes.
        """

        self.status_queue.put(f'Running {self.repository_name}...')

        # Set save parameters
        save_dir = kwargs.pop('save_dir', None)
        save_type = kwargs.pop('save_type', None)

        dataset_ids = self.get_dataset_ids(
            dataset_list_url=self.dataset_list_url,
            instance_path='p > a',
        )

        dataset_df = self.get_all_page_data(
            page_ids=dataset_ids,
        )

        results_dict = {'datasets': dataset_df}

        if save_dir and save_type:
            self.status_queue.put(f'Saving output to "{save_dir}.')
            save_results(
                results=results_dict,
                data_dir=save_dir,
                output_format=save_type
            )
            self.status_queue.put('Save complete.')

        self.status_queue.put(f'{self.repository_name} run complete.')
        self.continue_running = False

    def _clean_results(
            self,
            results: AttributeDict
    ) -> AttributeDict:
        """Applies parse functions to the results entries.

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
                pattern=re.compile(r'\d+-\d+-\d+')
            )
        if link_date:
            results['link_date'] = find_first_match(
                string=link_date,
                pattern=re.compile(r'\d+-\d+-\d+')
            )
        if num_citations:
            results['num_citations'] = parse_numeric_string(num_citations)
        if num_views:
            results['num_views'] = parse_numeric_string(num_views)

        return results

    @BaseCollector._pb_indeterminate
    def get_dataset_ids(
            self,
            dataset_list_url: str,
            instance_path: str
    ) -> list[str]:
        """Returns the dataset ids for all datasets on the given page.

        Parameters
        ----------
        dataset_list_url : str
            URL for page containing links to the datasets to scrape.
        instance_path : str
            CSS Selector path for the datasets on the page.

        Returns
        -------
        dataset_ids : list[str]
        """

        self.status_queue.put('Scraping dataset ids...')
        self.current_query_ref = 'Dataset ID\'s'

        self.driver.get(dataset_list_url)
        WebDriverWait(self.driver, 5).until(
            ec.presence_of_element_located((By.CSS_SELECTOR, instance_path))
        )

        soup = self._get_soup(features='html.parser')

        dataset_ids = [
            instance.attrs['href'].split('/')[-1]
            for instance in web_utils.get_variable_tags(
                soup=soup,
                path=instance_path
            )
        ]

        self.status_queue.put('Dataset id scraping complete.')

        return dataset_ids

    def _get_attribute_values(
            self,
            soup: bs4.BeautifulSoup
    ) -> AttributeDict:
        """Scrapes the possible attributes provided in the repository.

        Parameters
        ----------
        soup : BeautifulSoup

        Returns
        -------
        result_dict : dict
        """

        result_dict = dict()

        for var, attr in self.individual_attr_dict.items():
            result_dict[var] = web_utils.get_tag_value(
                web_utils.get_single_tag(
                    soup=soup,
                    class_type=re.compile(r''),
                    string=re.compile(attr)
                )
            )

        for var, attr in self.parent_attr_dict.items():
            result_dict[var] = [
                web_utils.get_tag_value(parent_subset)
                for parent in web_utils.get_parent_tag(
                    soup=soup,
                    string=re.compile(attr)
                )
                for parent_subset in parent.find_all(
                    name=re.compile('(p|span)')
                )
            ]

        for var, attr in self.sibling_attr_dict.items():
            result_dict[var] = web_utils.get_tag_value(
                web_utils.get_sibling_tag(soup, attr)
            )

        for var, attr in self.uncle_attr_dict.items():
            tag_texts = [
                web_utils.get_tag_value(
                    tag=tag,
                    separator='~'
                ).split('~')
                for tag in web_utils.get_parent_sibling_tags(
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
        url: str,
        clean: bool = True,
    ) -> AttributeDict:
        """Returns all data from the requested page.

        Parameters
        ----------
        url : str
        clean : bool, optional (default=True)

        Returns
        -------
        result_dict : dict
        """

        self.driver.get(url)

        for section, wait_path_str in self.wait_path_strings.items():
            # Wait for element to load
            try:
                WebDriverWait(self.driver, 10).until(
                    web_utils.text_to_be_present_on_page(wait_path_str)
                )
            except TimeoutException:
                self.status_queue.put(
                    f'Unable to locate \'{wait_path_str}\' '
                    f'on \'{url}\'.'
                )
                print(wait_path_str)

        soup = self._get_soup(features='html.parser')

        result_dict = self._get_attribute_values(soup)
        result_dict['url'] = self.driver.current_url

        is_external = web_utils.get_single_tag_from_tag_info(
            soup=soup,
            class_type='span',
            string='EXTERNAL'
        )

        # Get file info
        if not is_external:
            try:
                self.driver.find_element_by_link_text('Download').click()
            except NoSuchElementException:
                pass
            else:
                self.driver.switch_to.window(self.driver.window_handles[-1])

                soup = self._get_soup(features='html.parser')
                result_dict['files'] = [
                    web_utils.get_tag_value(child)
                    for child in web_utils.get_variable_tags(
                        soup=soup,
                        path='li > a'
                    )
                ]

                # Close new window and switch back to previous
                self.driver.close()
                self.driver.switch_to.window(self.driver.window_handles[0])

        if clean:
            result_dict = self._clean_results(result_dict)

        return result_dict

    def get_all_page_data(
        self,
        page_ids: Collection[str],
        clean: bool = True
    ) -> pd.DataFrame:
        """Returns data for all pages for the requested base url.

        Parameters
        ----------
        page_ids : list-like
            dataset ids to use for pulling up each page.
        clean : bool, optional (default=True)

        Returns
        -------
        dataset_df : pandas.DataFrame
        """

        self.status_queue.put('Scraping page data...')

        dataset_df = pd.DataFrame()

        for page_id in self._pb_determinate(page_ids):
            url = f'{self.base_url}/{page_id}'

            results = self.get_individual_page_data(
                url=url,
                clean=clean
            )
            dataset_df = pd.concat([dataset_df, pd.DataFrame([results])])

        self.status_queue.put('Scraping complete.')

        dataset_df = dataset_df.reset_index(drop=True)
        return dataset_df
