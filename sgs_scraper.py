from time import sleep
from bs4 import BeautifulSoup
import requests
from utils import extract_table_data, export_to_excel, prepare_url
from pandas import to_datetime


def convert_to_date(dataframe):
    try:
        dataframe['Publication Date'] = to_datetime(dataframe['Publication Date'], format="%B %d, %Y",
                                                    errors='coerce').dt.date
    except KeyError:
        print("Unable to convert Publication date column to date..")
    return dataframe


if __name__ == "__main__":
    PRODUCT_RECALLS_URL = "https://campaigns.sgs.com/en/vr/product-recalls-light"
    SLEEP_TIME = 0     # in seconds
    EXPORT_EXCEL_FILENAME = "data/sgs_data.xlsx"
    requests_header = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/120.0.0.0 Safari/537.36"
    }

    current_url = PRODUCT_RECALLS_URL
    master_data = []

    try:
        while True:
            print(f"processing {current_url}")
            try:
                response = requests.get(current_url, headers=requests_header)
            except KeyboardInterrupt:
                break
            except:
                print(f"unable to get html from {current_url}")
                continue

            if not response.ok:
                print(f"unable to get requested page for url {current_url}")
                continue

            soup = BeautifulSoup(response.content, "html.parser")
            table_soup = soup.find("table", class_="table table--simple table--narrow")

            data = extract_table_data(table_soup)
            master_data.extend(data)
            base_url = "https://campaigns.sgs.com/en/vr/product-recalls-light/record?p={" \
                       "pg_no}&d=0&id=18CD45C15541&dc=http&lb=&rec={rec_no}"

            next_page = soup.find("li", class_="next")

            if not next_page:
                print("Scrapped all the urls")
                break

            next_page_url = prepare_url(PRODUCT_RECALLS_URL, next_page.a.attrs.get("href"))
            current_url = next_page_url
            sleep(SLEEP_TIME)

    except KeyboardInterrupt:
        print("Received Keyboard interrupt, stopping the scrapper..")

    except Exception as e:
        print(e)

    finally:
        # Export the master data to excel
        export_to_excel(master_data, EXPORT_EXCEL_FILENAME, convert_to_date)
        print(f"Data exported to {EXPORT_EXCEL_FILENAME}")
