from typing import Union, Callable
from urllib.parse import urlsplit
from bs4 import BeautifulSoup
import pandas as pd
import requests


def extract_table_data(table_soup: BeautifulSoup) -> list:
    """
    A generic function to extract the table data from html if format is as below
        <thead>
            <th>anc</th>    # table header
        </thead>
        <tbody>
            <tr>
                <td>value</td>      # header value
            </tr>
        </tbody>

    :param table_soup: the filtered bs4 object with table data
    :return: list [{}, {}, {}]
    """
    data = []

    # extract all the headers from table
    headers = []
    headers_soup = table_soup.find("thead")
    for header in headers_soup.find_all("th"):
        headers.append(header.text.strip())

    # extract the table data
    tbody_soup = table_soup.find("tbody")
    for row in tbody_soup.find_all("tr"):
        values = []
        for col in row.children:
            if col != "\n":
                values.append(col.text.strip())
        row_data = dict(zip(headers, values))
        data.append(row_data)
    return data


def prepare_url(baseurl: str, join_url: str) -> str:
    """
    Function to join the relative url and make it absolute url

    :param baseurl: the base url. e.g. https://google.com/en/
    :param join_url: the url which need to be joined with base url. e.g. /api/graph?page=5
    :return: str, full url
    """
    splitted_url = urlsplit(baseurl)
    return f"{splitted_url.scheme}://{splitted_url.netloc}{join_url}"


def clean_df(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Function to manipulate the dataframe before saving it.

    :param dataframe:
    :return: dataframe
    """
    return dataframe


def export_to_excel(data: list, excel_filename: str, validate_func: Callable = clean_df) -> bool:
    """
    Function to export the data into Excel file

    :param data: data which needs to be exported. format [{}, {}, {}]
    :param excel_filename: sgs_data.xlsx
    :param validate_func: expects a function which will take dataframe as argument and return dataframe.
    :return: True
    """
    df = pd.DataFrame(data)
    df = validate_func(df)
    df.to_excel(excel_filename)
    return True


def get_soup(url: str) -> Union[BeautifulSoup, None]:
    """
    Function to fetch the url and convert it to BeautifulSoup object. If error while fetching return None
    :param url:
    :return: BeautifulSoup object or None
    """
    requests_header = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/120.0.0.0 Safari/537.36"
    }

    try:
        response = requests.get(url, headers=requests_header)

        if not response.ok:
            print("unable to get 200 status from url..", response)
            return

        return BeautifulSoup(response.content, "html.parser")

    except KeyboardInterrupt:
        raise KeyboardInterrupt

    except Exception as e:
        print(e)
        return


def convert_to_date(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Function to convert the "Publication Date" column into date format.

    :param dataframe:
    :return: dataframe
    """
    try:
        dataframe['Publication Date'] = pd.to_datetime(dataframe['Publication Date'], format="%B %d, %Y",
                                                       errors='ignore').dt.date
    except KeyError:
        print("Unable to convert Publication date column to date..")
    return dataframe
