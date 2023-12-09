"""
Automatically insert data into the database.
"""

import json
import logging
import psycopg2

DB_URL = ""

CATEGORIES_IDS = {
    "carbonated-drinks": 1,
    "juices-and-fruit-drinks": 2,
    "sweets-and-chocolate": 3,
    "tea-coffee": 4,
    "skin-and-hair-care": 5,
    "kitchen-bathroom": 6
}

QUERY = "INSERT INTO products (name, image_link, ean, number_of_pieces, number_of_cases, number_of_pieces_on_pallet, is_packaged, is_palletized, category_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"

logging.config(level=logging.INFO)

def get_data() -> list[dict]:
    with open('./data.json', 'r') as f:
        data = json.load(f, strict=False)
    return data


def is_product_correct(to_check: dict) -> bool:
    return list(to_check.keys()) in (["Name", "Category", "ImageLink", "EAN", "Number pieces in packaging", "Number of cases on the pallet", "Number of pieces on the pallet"], ["Name", "Category", "ImageLink", "EAN", "Number of pieces in the package", "Number of packages on a pallet", "Number of pieces on a pallet"])
    
    
def is_issued(issued_product: dict) -> bool:
    return any(key in issued_product for key in ["Name", "Category", "ImageLink"])


def set_ean(to_check: dict) -> str:
    product = to_check.copy()
    if "EAN" not in to_check:
        product["EAN"] = None
    return product

    
def insert_data(data: list[dict], is_issued: bool = False) -> None:
    global cur
    product = set_ean(data)
    category_id = CATEGORIES_IDS[product["Category"]]
    
    if is_issued:
        cur.execute(QUERY, (product["Name"], product["ImageLink"], product["EAN"], None, None, None, False, False, category_id))
    elif "Number pieces in packaging" in product:
        cur.execute(QUERY, (product["Name"], product["ImageLink"], product["EAN"], product["Number pieces in packaging"], product["Number of cases on the pallet"], product["Number of pieces on the pallet"], True, False, category_id))
    elif "Number of pieces in the package" in product:
        cur.execute(QUERY, (product["Name"], product["ImageLink"], product["EAN"], product["Number of pieces in the package"], product["Number of packages on a pallet"], product["Number of pieces on a pallet"], True, False, category_id))
    
    
def run():    
    global cur
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    
    data = get_data()
    issued = []
    
    for product in data:
        if is_product_correct(product):
            insert_data(product, cur)
        else:
            issued.append(product)        

    for issued_product in issued:
        if is_issued(issued_product):
            insert_data(issued_product, cur, is_issued=True)
        else:
            print(issued_product)

    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    run()
    