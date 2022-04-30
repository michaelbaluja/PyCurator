# PyCurator
Making data extraction and curation as easy as py.

PyCurator allows users to easily query research repositories without the trouble of reading through API
documentation. Data curation is now as easy as ```$ pycurator```. Whether you want the ease of clicking
some buttons and getting the data or the flexibility of modifying query format, PyCurator provides a simple
UI for quickly retrieving data that is built on top of an extensible collection of Web and API scraper classes.

## Supported Repositories
PyCurator currently supports the following repositories in the capacities listed. Authentication is only required for Kaggle,
though may provide runtime benefits for Dryad as rate-limiting is relaxed.
 

| Repository           | Authentication                                                                               |
|----------------------|----------------------------------------------------------------------------------------------|
| Dataverse            |                                                                                              |
| Dryad                | [Dryad](https://github.com/CDL-Dryad/dryad-app/blob/main/documentation/apis/api_accounts.md) |
| Figshare             |                                                                                              |
| Kaggle               | [Kaggle](https://www.kaggle.com/docs/api#authentication)                                     |
| OpenML               |                                                                                              |
| Papers With Code     |                                                                                              |                
| Zenodo               |                                                                                              |

If there's a repository that you would like to see added to the list, check out the [Contributions](#contributions) section.

## Installation and use
### Installation
Dependencies are provided in the ```requirements.txt``` file.
It is recommended to create a virtual environment to ensure there is no conflict with the packages
in your current work space.

PyCurator requires a Python version >= 3.10.

To run, simply paste the following commands into your terminal
```bash
git clone https://github.com/michaelbaluja/PyCurator.git
cd PyCurator
python -m pip install -e .
pycurator
```

### Use
#### Repository Selection
After following the commands above, you will be met with the landing page, containing licensing, funding, and 
copyright information. Clicking ```Continue``` will bring you to the following page 

![Repository Selection Page](/images/repo_selection.png "Repository Selection Page")

#### Parameter Selection
Clicking on one of the repositories will bring up the respective parameters used for querying the API and 
saving your results. Parameters will vary depending on repository selected.

![Parameter Selection](/images/param_selection.png "Figshare Parameter Selection")

These parameters are outlined as
| Parameter      | Description                                                                                                                      |
|----------------|----------------------------------------------------------------------------------------------------------------------------------|
| Save Directory | Location to save results. Defaults to "/data/{repo_name}/{search_term}_{search_type}.json" within PyCurator /data sub-directory. |
| Search Terms   | Search term(s) to query. Terms should be separated with a comma, and multi-word terms should be wrapped in quotes.               |
| Search Types   | Type of objects to query.                                                                                                        |
After all required parameters are provided, the ```Run``` button is activated.

#### Run Page
![Run Page](/images/run_page.png "Run Page")

The run page provides high level status updates in the main window. These include the beginning and end
of processes, rate-limiting issues, runtime completion, and saving confirmation. Below are real-time status updates for the 
specific query being completed as well as a progress bar for the high level task. During tasks that have
a fixed duration, such as metadata querying or some web scraping, a fixed-length progress bar will show
the progression of output. During tasks that have an indeterminate duration, a cycling task bar will be 
present to represent continued progress.

At the bottom are the navigation buttons. To avoid unnecessary queries, the ```Back``` button is unresponsive 
during runtime, but is activated after completion. The ```Stop``` button is used to interrupt runtime and stop querying.
After runtime completion or interruption, the ```Stop``` button is replaced by the ```Exit``` button, allowing you to 
safely terminate the program.

## Contributions
### Bugs
Please note that as of Spring 2022, PyCurator is still undergoing active development. For any bugs or problems that you come across, open an issue that details the problem that 
you're experiencing.

### Extension
Know of an API that you think should be included in PyCurator? Create a Pull Request outlining
the API and why you think it would be beneficial, and make sure to follow the format set out
through the existing Scraper classes.

## Funding and acknowledgements
The initial development of this program was funded by the Librarians Association of The University of California (LAUC) and UC San Diego Library Research Data Curation Program (RDCP).

Thank you to Matt Peters, Dan LaSusa, John Chen, Joshua Weimer, and Amy Ly for their feedback during testing of early iterations of PyCurator.
