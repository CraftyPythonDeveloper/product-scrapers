from tqdm import tqdm
from typing import Union
from bs4 import BeautifulSoup
import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor
import os
import sys


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
                                                       errors='coerce').dt.date
    except:
        print("Unable to convert the publication date to date format..")
    return dataframe


def product_link_generator(end: int = 100) -> list:
    """
    Function to generate the product link by following the url pattern.

    :param end: No of products. default is 100
    :return: list
    """
    base_url = "https://campaigns.sgs.com/en/vr/product-recalls-light/record?p={pg_no}" \
               "&d=0&id=18CD45C15541&dc=http&lb=&rec={rec_no}"

    links = []
    for i in range(0, end, 10):
        for rec in range(10):
            links.append(base_url.format(pg_no=i, rec_no=rec))
    return links[:end]


def get_product_data(bs_soup: BeautifulSoup) -> dict:
    table = bs_soup.find("div", class_="table-wrapper table-wrapper-pairs")

    data = {"Product Name": ""}
    for row in table.find_all("tr"):
        header = row.find("th").text
        value = row.find("td").text
        if header == "Image":
            value = row.find("td").img.attrs.get("src")
        data[header] = value

    return data


def get_total_record_count():
    count = 12_421
    url = "https://campaigns.sgs.com/en/vr/product-recalls-light"
    page_soup = get_soup(url)
    record_count_info = page_soup.find("div", class_="grid grid--2").text.strip()

    for word in record_count_info.split(" "):
        try:
            count = int(word)
        except ValueError:
            pass
    return count


def get_data(url):
    # print(url)
    soup = get_soup(url)

    if not soup:
        return

    table_data = get_product_data(soup)
    table_data["Product Name"] = soup.find("div", class_="page-header").text.strip()
    table_data["original recall notice url"] = soup.find("p").a.attrs.get("href")
    table_data["page_url"] = url
    return table_data


if __name__ == "__main__":
    print("Script started..")
    try:
        max_worker = int(sys.argv[1])
    except (IndexError, ValueError):
        max_worker = 20

    SLEEP_TIME = 0     # in seconds
    EXPORT_EXCEL_FILENAME = "data/sgs_data_extended.xlsx"
    requests_header = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/120.0.0.0 Safari/537.36"
    }

    if not os.path.exists("data"):
        os.mkdir("data")

    total_number_of_records = get_total_record_count()
    df = pd.DataFrame()
    if os.path.exists(EXPORT_EXCEL_FILENAME):
        df = pd.read_excel(EXPORT_EXCEL_FILENAME, index_col=0)
        if df.shape[0] == int(total_number_of_records):
            print("All data is up to date, No new data to scrape..")
            input("press any key to exit..")
            sys.exit()
        total_number_of_records = int(total_number_of_records) - df.shape[0]
    print(f"{total_number_of_records} new products found. scraping..")
    product_links = product_link_generator(total_number_of_records)

    master_data = []

    with ThreadPoolExecutor(max_workers=max_worker) as executor:
        futures = [executor.submit(get_data, url) for url in product_links]
        for future in tqdm(futures):
            try:
                master_data.append(future.result())
            except Exception:
                pass

    # Export the master data to excel
    new_df = pd.DataFrame(master_data)
    if not df.empty:
        new_df = pd.concat([new_df, df]).reset_index(drop=True)
    new_df = convert_to_date(new_df)
    new_df.to_excel(EXPORT_EXCEL_FILENAME)
    print(f"Data exported to {EXPORT_EXCEL_FILENAME}")
    input("Press any key to exit..")
