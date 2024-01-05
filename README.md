# Overview
* The first script is **sgs_scraper.py**, this script can be used if you only want to scrape the overview of the product. This will scrape 10 records per page.
* The second script is **sgs_scraper_v2.py**, this script can be used if you want to scrape all the data related to product. This will run slower as compared to first script because this will scrape 1 record per page.


# Setup And Installation

### Python Installation
1. If you have python setup already you can skip this step.
2. Install the python if not installed already. Download it from [python.org](https://www.python.org/downloads/)
3. To check if python is successfully installed, open terminal or cmd and type <br> ```python --version``` or ```python3 --version```
4. Optional step: create virtual environment. Click on [venv](https://packaging.python.org/en/latest/guides/installing-using-pip-and-virtual-environments/) to see how to create venv.


### Script Dependency Installation Guide
1. Optional step: If you are using venv then activate it first.
2. In terminal **cd** to the project directory and type ```pip install -r requirements.txt``` to install script dependencies.


### Script Running Guide
1. To run the script type ```python sgs_scraper.py```  to start the **overview** data scrapping.
2. To scrape the product level data, type ```python sgs_scraper_v2.py```
