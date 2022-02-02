# PyCurator
Making data extraction and curation as easy as py.

PyCurator allows users to easily query research repositories without the trouble of reading through API
documentation. Data curation is now as easy as ```$ python main.py```. Whether you want the ease of clicking
some buttons and getting the data or the flexibility of modifying query format, PyCurator provides a simple
UI for quickly retrieving data that is built on top of an extensible collection of Web and API scraper classes.

## Supported Repositories
PyCurator currently supports the following repositories in the capacities listed.

| Repository           | API                | Web                |
|----------------------|--------------------|--------------------|
| Dataverse            | :white_check_mark: | :white_check_mark: |
| Dryad                | :white_check_mark: | :white_check_mark: |
| Figshare             | :white_check_mark: | :x:                |
| Kaggle               | :white_check_mark: | :x:                |
| OpenML               | :white_check_mark: | :white_check_mark: |
| Papers With Code     | :white_check_mark: | :x:                |
| UCI Machine Learning | :x:                | :white_check_mark: |
| Zenodo               | :white_check_mark: | :x:                |

If there's a repository that you would like to see added to the list, check out the [Contributions](#contributions) section.

## Installation and use
### Installation
Required depencencies are listed below in the [Dependencies](#dependencies) section.
It is recommended to create a virtual environment to ensure there is no conflict with the packages
in your current work space.

To run, simply paste the following commands into your terminal
```bash
git clone https://github.com/michaelbaluja/PyCurator.git
cd PyCurator
python main.py
```

### Use
#### Repository Selection
After following the commands above, you will be met with the landing page, containing licensing, funding, and 
copyright information. Clicking ```Continue``` will bring you to the following page 

![Repository Selection Page](/images/repo_selection.png "Repository Selection Page")

#### Parameter Selection
Clicking on one of the repositories will bring up the respective parameters used for querying the API and 
saving your results.

![Parameter Selection](/images/param_selection_1.png "Dataverse Parameter Selection")
![Parameter Selection](/images/param_selection_2.png "UCI Parameter Selection")

These parameters are outlined as
| Parameter      | Description                                                                                                                                                                             |
|----------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Save Directory | Location to save results, defaults to "/data/{repo_name}/{search_term}_{search_type}.json".                                                                                             |
| Flatten Output | Flag for flattening nested json results from the API.                                                                                                                                   |
| Web Scrape     | Flag for querying additional data via web scraping that is not available through the API.  This is an optional parameter for repositories that are not predominately web-scraper based. |
| Search Terms   | Search term(s) to query. Terms should be separated with a comma, and multi-word terms should be wrapped in quotes.                                                                      |
| Search Types   | Type of objects to query.                                                                                                                                                               |
After all required parameters are provided, the ```Run``` button is activated.

#### Run Page
![Run Page](/images/run_page.png "Run Page")

The run page provides high level status updates in the main window. These include the beginning and end
of processes, rate-limiting issues, runtime completion, and saving confirmation. Below are real-time status updates for the 
specific query being completed as well as a progress bar for the high level task. During tasks that have
a fixed duration, such as metadata querying or some webscraping, a fixed-length progress bar will show
the progression of output. During tasks that have an indeterminate duration, a cycling task bar will be 
present to represent continued progress.

At the bottom are the navigation buttons. To avoid unnecessary queries, the ```Back``` button is unresponsive 
during runtime, but is activated after completion. The ```Stop``` button is used to interrupt runtime and stop querying.
After runtime completion or interruption, the ```Stop``` button is replaced by the ```Exit``` button, allowing you to 
safely terminate the program.

### Dependencies
- bs4
- flatten_json
- kaggle
- openml
- pandas
- requests
- selenium
- webdriver_manager

## Contributions
### Bugs
For any bugs or problems that you come across, open an issue that details the problem that 
you're experiencing.

### Extension
Know of an API that you think should be included in PyCurator? Create a Pull Request outlining
the API and why you think it would be beneficial, and make sure to follow the format set out
through the existing Scraper classes.

## Funding
The initial development of this program was funded by the Librarians Association of the University of California (LAUC) and UC San Diego Library Research Data Curation Program (RDCP).
