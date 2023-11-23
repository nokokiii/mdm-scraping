"""
Gathering data from http://www.mdmbrands.pl/ and saving it to JSON file
"""

import json
import time
import requests
import bs4

CATEGIRIES = ["carbonated-drinks", "juices-and-fruit-drinks", "sweets-and-chocolate", "tea-coffee", "skin-and-hair-care", "kitchen-bathroom"]


def get_products_urls(url: str) -> list:
    """
    Get all products urls from category page
    """
    print("Getting product from: ", url)

    page_source = requests.get(url)
    soup = bs4.BeautifulSoup(page_source.text, "html.parser")
    products = soup.find_all("a", {"class": "woocommerce-LoopProduct-link woocommerce-loop-product__link"})
    products_urls = [product["href"] for product in products]

    print(f"Found {len(products_urls)} products")
    
    return products_urls

    
def get_produts_info(url: str, category: str) -> dict:
    """
    Get info about product from product page
    """
    page_source = requests.get(url)
    soup = bs4.BeautifulSoup(page_source.text, "html.parser")
    name = soup.find("h1", {"class": "product_title entry-title elementor-heading-title elementor-size-default"})
    product_info = {
        "Category": category,
        "ImageLink": "",
        "EAN": "",
        "Name": name.text,
    }
    try:
        table = soup.find("table")
        table = table.find("tbody")
    except Exception as e:
        print("Something went wrong with table gathering", e)

    try:
        for row in table.find_all("tr"):
            row = row.find_all("td")
            product_info[row[0].text] = int(row[1].text)


    except Exception as e:
        print("Something went wrong with table info gathering", e)


    product_img = soup.find('img', {'class': 'wp-post-image'})
    product_info["ImageLink"] = product_img["src"]

    return product_info


def main():
    json_data = []

    start = time.time()

    for category in CATEGIRIES:
        category_url = f"http://www.mdmbrands.pl/kategoria-produktu/{category}/"
        products = get_products_urls(category_url)

        products_info = [get_produts_info(product, category) for product in products]
        print(f"Added {len(products_info)} products to JSON")
        json_data.extend(products_info)

    with open("./data.json", "w") as f:
        json.dump(json_data, f, indent=4, ensure_ascii=False)

    print("JSON saved")

    end = time.time()

    print(f"Done in: {end - start}s")


if __name__ == "__main__":
    main()
