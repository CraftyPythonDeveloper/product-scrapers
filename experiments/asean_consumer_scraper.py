import os
import sys

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import pandas as pd
from concurrent.futures import ThreadPoolExecutor


def get_products_overview(max_products=10_000):
    api = "https://www.aseanconsumer.org/product-alert-datatable"
    payload = "draw=1&columns%5B0%5D%5Bdata%5D=recall_date&columns%5B0%5D%5Bname%5D=recall_date" \
              "&columns%5B0%5D%5Bsearchable%5D=true&columns%5B0%5D%5Borderable%5D=true&columns" \
              "%5B0%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B0%5D%5Bsearch%5D%5Bregex%5D=false" \
              "&columns%5B1%5D%5Bdata%5D=picture&columns%5B1%5D%5Bname%5D=picture&columns%5B1%5D" \
              "%5Bsearchable%5D=false&columns%5B1%5D%5Borderable%5D=false&columns%5B1%5D%5Bsearch" \
              "%5D%5Bvalue%5D=&columns%5B1%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B2%5D%5Bdata" \
              "%5D=name&columns%5B2%5D%5Bname%5D=name&columns%5B2%5D%5Bsearchable%5D=true&columns" \
              "%5B2%5D%5Borderable%5D=true&columns%5B2%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B2%5D" \
              "%5Bsearch%5D%5Bregex%5D=false&columns%5B3%5D%5Bdata%5D=type&columns%5B3%5D%5Bname" \
              "%5D=type&columns%5B3%5D%5Bsearchable%5D=true&columns%5B3%5D%5Borderable%5D=true" \
              "&columns%5B3%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B3%5D%5Bsearch%5D%5Bregex%5D" \
              "=false&columns%5B4%5D%5Bdata%5D=model_product&columns%5B4%5D%5Bname%5D" \
              "=model_product&columns%5B4%5D%5Bsearchable%5D=true&columns%5B4%5D%5Borderable%5D" \
              "=true&columns%5B4%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B4%5D%5Bsearch%5D%5Bregex%5D" \
              "=false&columns%5B5%5D%5Bdata%5D=country&columns%5B5%5D%5Bname%5D=country&columns" \
              "%5B5%5D%5Bsearchable%5D=true&columns%5B5%5D%5Borderable%5D=true&columns%5B5%5D" \
              "%5Bsearch%5D%5Bvalue%5D=&columns%5B5%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B6" \
              "%5D%5Bdata%5D=jurisdiction_of_recall&columns%5B6%5D%5Bname%5D" \
              "=jurisdiction_of_recall&columns%5B6%5D%5Bsearchable%5D=true&columns%5B6%5D" \
              "%5Borderable%5D=true&columns%5B6%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B6%5D" \
              "%5Bsearch%5D%5Bregex%5D=false&columns%5B7%5D%5Bdata%5D=original_alert&columns%5B7" \
              "%5D%5Bname%5D=original_alert&columns%5B7%5D%5Bsearchable%5D=true&columns%5B7%5D" \
              "%5Borderable%5D=true&columns%5B7%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B7%5D" \
              "%5Bsearch%5D%5Bregex%5D=false&order%5B0%5D%5Bcolumn%5D=0&order%5B0%5D%5Bdir%5D" \
              "=desc&start=0&length=100000"

    headers = {
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'accept': 'application/json',
        'authority': 'www.aseanconsumer.org',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/120.0.0.0 Safari/537.36'
    }

    response = requests.post(api, data=payload, headers=headers)
    if not response.ok:
        print("unable to get 200 status from url..", response)
        return

    return response.json()


def parse_table(soup):
    all_td = soup.find_all("td")
    headers = [i.text.strip() for i in all_td[::2]]
    values = [i.text.strip() for i in all_td[1::2]]
    return dict(zip(headers, values))


def get_data(url):
    try:
        response = requests.get(url)
    except:
        return url
    soup = BeautifulSoup(response.text, "html.parser")
    data = dict()
    for i in soup.find_all("table", class_="table-product-alert"):
        data.update(parse_table(i))
    return data


if __name__ == "__main__":
    print("Script started..")
    try:
        max_worker = int(sys.argv[1])
    except (IndexError, ValueError):
        max_worker = 15

    if not os.path.exists("data"):
        os.mkdir("data")

    EXPORT_EXCEL_FILENAME = "data/asean_consumers.xlsx"
    overview = get_products_overview()
    urls = ["https://www.aseanconsumer.org/product-" + row["slug"] for row in overview["data"]]

    master_data = []
    failed_urls = []
    try:
        # for url in tqdm(urls):
        with ThreadPoolExecutor(max_workers=max_worker) as executor:
            futures = [executor.submit(get_data, url) for url in urls]

            for future in tqdm(futures):
                result = future.result()
                if isinstance(result, dict):
                    master_data.append(result)
                else:
                    failed_urls.append(result)

    except Exception as e:
        print(e)
    finally:
        pd.DataFrame(master_data).to_excel(EXPORT_EXCEL_FILENAME)
        print(f"Saved the data to {EXPORT_EXCEL_FILENAME}")
        input("press any key to exit..")
