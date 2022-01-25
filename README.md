# Scripts related to: A Review of Publishing and Sharing Practices for Machine Learning Objects for Informing Library Curation Practices

## Project Overview
The goal of this project is to inform internal and community practices for curating machine learning (ML) objects, so that the relationships between the components of ML objects—experiments, tasks, algorithms, datasets, features, models, hardware/software platforms, parameters, and evaluation measures—are made explicit and can be readily understood or reused when the objects are accessed by future users. To that end, this project documents and assesses the way ML objects are currently shared in scientific repositories, to identify commonalities in structure and documentation as well as any apparent barriers to reuse. These barriers may include, for example, missing components (for a given task), idiosyncrasies in labeling (or lack thereof) of components, and variability in file organization. By identifying the core components of various ML objects and their needed documentation, we hope to move the community toward a common practice for curation as well as to increase efficiency by facilitating interoperability and reuse of ML components.

The specific objectives for this project are to survey ML research objects currently published in common scientific data repositories, to assess: 1) which components of the ML lifecycle are being shared and 2) how ML objects are being documented. In evaluating this compiled information, we further aim to 3) formally specify the core features necessary in curated ML objects to enable reuse by a user with basic knowledge of ML.

This GitHub repository contains a unified search tool along with scripts used to query data from the following repositories:

**ML-focused repositories**
* [Kaggle](https://www.kaggle.com/)
  * Kaggle is a popular ML and data science platform, frequently used both by ML newcomers as well as those with more experience. The platform hosts datasets as well as data analysis notebooks. Documentation can be variable, but components are intended to be reused, meaning documentation provided is “minimally viable” in terms of reuse.
* [OpenML](https://www.openml.org/)
  * OpenML is a platform with the goal to "build an open, organized, online ecosystem for machine learning." The platform provides datasets, machine learning tasks performed on the datasets, specific algorithms (flows) that are ran to complete the task, and individual runs of a task. There is a slight barrier due to the requirement of ARFF format for datasets. While the required information necessary for dataset upload is sparse, descriptive dataset properties are automatically extracted and made available.
* [Papers With Code](https://paperswithcode.com/)
  * Papers With Code is a platform aiming "to create a free and open resource with Machine Learning papers, code and evaluation tables." All content on the website is openly licensed under CC-BY-SA. There is no requirement that papers are uploaded with their code implementation, although due to the open nature of the website, anyone is allowed to upload their own unofficial implementation.
* [UC Irvine Machine Learning Repository](https://archive-beta.ics.uci.edu)
  * This popular ML repository primarily hosts training and test datasets, domain theories, and data generators, with broad variability in documentation. Useful for identifying which properties of ML datasets are documented when reuse is anticipated.

**Generalist repositories**
* [Figshare](https://figshare.com/)
  * A non-curated generalist repository, with a soft limit of 20 GB. Offers several Creative Commons and open source licenses.
* [Zenodo](https://zenodo.org/)
  * A non-curated generalist repository, free of charge up to 50 GB per dataset. Integration with GitHub allows users to publish their GitHub repositories easily. Offers hundreds of open licenses.
* [Dryad](https://datadryad.org/stash)
  * A lightly curated generalist repository for datasets, with a soft limit of 300 GB/dataset. Assigns the CC0 public domain dedication to all submissions.
* [UC San Diego Library Digital Collections](https://library.ucsd.edu/dc)
  * Contains a small number of ML research objects, which have been lightly curated. Offers a variety of licenses; default is CC-BY.
* [Harvard Dataverse](https://dataverse.harvard.edu/)
  * Offers tiered curation with deposits and a limit of 1 TB. In addition to serving as a repository for research data, depositors can submit their data and code as a container “dataverse” with all necessary data and metadata. CC0 is highly encouraged, and applied by default.

The UC Irvine Machine Learning Repository and UC San Diego Library Digital Collections do not have public-facing APIs. Data from the UC Irvine Machine Learning Repository is collected through the use of web-scraping, and data from the UC San Diego Library Digitial Collections was collected directly from the repository owners.

## Data Collection
Data collection for each repository is available both through individual Jupyter [Notebooks](https://github.com/stephlabou/LAUC_ML/tree/main/notebooks) as well as the general user interface in ```main.py```. Each notebook contains repository-specific code and instructions to query and extract metadata for all objects matching specified search terms.

Each repository is classified based on its format for collecting data. In this manner, there are repositories that search by search term (term\_scrapers), search type (type\_scrapers), search term and search type (term\_type\_scrapers), and those that search by web scraping (web\_scrapers). Due to API limitations, web scraping is required alongside traditional API calls for some repositories, though these are still classified by the above format.

## Installation and use
### General
All scripts and tools used in this project are written in Python, with compatability tested as old as Python 3.7. To install Python, follow the instructions at [python.org](https://www.python.org/downloads/). 

For macOS useres, while Python 2 is installed by default, Python 3 must also be installed for compatability reasons. If you are installing and using Python 3 on macOS, you must use the ```python3``` command instead of ```python``` when entering the following installation and use commands.

To download the files necessary for use, click on the green ```code``` button near the top of the page and click *"Download ZIP"*.

Additional python packages are required for installation, and are noted in the ```setup.py``` file. To easily install all requirements, you can use the command
```bash
$ python setup.py install
```
from your command terminal.

### User Interface
To run the UI, navigate to the repository folder on your command terminal and run the following command:
```bash
$ python main.py
```

# TODO: Add UI stuff

### Notebooks
To utilize the repository scraper notebooks, ensure that you have Jupyter installed, and simply open the ```repository_name.ipynb``` file via Jupyter Notebooks or JupyterLab. This option is beneficial for running individual repository scrapers where you may wish to tweak the commands (such as for viewing intermediate data results).

Instructions on installation and use for Jupyter is available [here](https://jupyter.org/install).

## Funding
This project is funded by the Librarians Association of the University of California (LAUC).
