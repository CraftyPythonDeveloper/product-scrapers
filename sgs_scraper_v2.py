from bs4 import BeautifulSoup
from utils import (export_to_excel, get_soup, convert_to_date,)


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


if __name__ == "__main__":
    SLEEP_TIME = 0.1     # in seconds
    EXPORT_EXCEL_FILENAME = "data/sgs_data_extended.xlsx"
    requests_header = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/120.0.0.0 Safari/537.36"
    }

    total_number_of_records = get_total_record_count()
    product_links = product_link_generator(total_number_of_records)

    master_data = []

    try:
        for link in product_links:
            print(link)
            soup = get_soup(link)

            if not soup:
                continue

            table_data = get_product_data(soup)
            table_data["Product Name"] = soup.find("div", class_="page-header").text.strip()
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
