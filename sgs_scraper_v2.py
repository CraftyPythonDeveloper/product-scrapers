from bs4 import BeautifulSoup
import requests
from utils import export_to_excel
from pandas import to_datetime


def product_link_generator(end: int = 100) -> list:
    """
    Function to generate the product link by following the url pattern.

    :param end: No of products. default is 100
    :return: list
    """
    base_url = "https://campaigns.sgs.com/en/vr/product-recalls-light/record?p={pg_no}" \
               "&d=0&id=18CD45C15541&dc=http&lb=&rec={rec_no}"

    links = []
    for i in range(10, end, 10):
        for rec in range(10):
            links.append(base_url.format(pg_no=i, rec_no=rec))
    return links


def get_product_data(bs_soup: BeautifulSoup) -> dict:
    table = bs_soup.find("div", class_="table-wrapper table-wrapper-pairs")

    data = {"Publication Date": ""}
    for row in table.find_all("tr"):
        header = row.find("th").text
        value = row.find("td").text
        if header == "Image":
            value = row.find("td").img.attrs.get("src")
        data[header] = value

    return data


def convert_to_date(dataframe):
    try:
        dataframe['Publication Date'] = to_datetime(dataframe['Publication Date'], format="%B %d, %Y",
                                                    errors='coerce').dt.date
    except KeyError:
        print("Unable to convert Publication date column to date..")
    return dataframe


if __name__ == "__main__":
    TOTAL_NUMBER_OF_RECORDS = 12_421
    SLEEP_TIME = 0.1     # in seconds
    EXPORT_EXCEL_FILENAME = "data/sgs_data_extended.xlsx"
    requests_header = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/120.0.0.0 Safari/537.36"
    }

    product_links = product_link_generator(TOTAL_NUMBER_OF_RECORDS)

    master_data = []

    try:
        for link in product_links:
            print(link)
            try:
                response = requests.get(link)
            except KeyboardInterrupt:
                break
            except:
                print(f"unable to get html from {link}")
                continue

            if not response.ok:
                print(response)
                continue

            soup = BeautifulSoup(response.content, "html.parser")
            table_data = get_product_data(soup)
            table_data["'Publication Date'"] = soup.find("div", class_="page-header").text.strip()
            table_data["original recall notice url"] = soup.find("p").a.attrs.get("href")
            master_data.append(table_data)

    except KeyboardInterrupt:
        print("Received Keyboard interrupt, stopping the scrapper..")

    except Exception as e:
        print(e)

    finally:
        # Export the master data to excel
        export_to_excel(master_data, EXPORT_EXCEL_FILENAME, convert_to_date)
        print(f"Data exported to {EXPORT_EXCEL_FILENAME}")
