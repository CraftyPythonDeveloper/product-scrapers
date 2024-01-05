from time import sleep
from utils import (extract_table_data, export_to_excel, prepare_url, get_soup, convert_to_date,)


if __name__ == "__main__":
    PRODUCT_RECALLS_URL = "https://campaigns.sgs.com/en/vr/product-recalls-light"
    SLEEP_TIME = 0     # in seconds
    EXPORT_EXCEL_FILENAME = "data/sgs_data.xlsx"

    current_url = PRODUCT_RECALLS_URL
    master_data = []

    try:
        while True:
            print(f"processing {current_url}")
            soup = get_soup(current_url)

            if not soup:
                continue

            table_soup = soup.find("table", class_="table table--simple table--narrow")
            data = extract_table_data(table_soup)
            master_data.extend(data)

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
