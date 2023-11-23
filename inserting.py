"""
Automatically insert data into the database.
"""

import json
import psycopg2

DB_NAME = "db_name"
USER = "username"
PASSWORD = "password"
HOST = "host"
PORT = "port"

CATEOGRIES_IDS = {
    "carbonated-drinks": 1,
    "juices-and-fruit-drinks": 2,
    "sweets-and-chocolate": 3,
    "tea-coffee": 4,
    "skin-and-hair-care": 5,
    "kitchen-bathroom": 6
}

QUERY = "INSERT INTO products (name, image_link, ean, number_of_pieces, number_of_cases, number_of_pieces_on_pallet, is_packaged, is_palletized, category_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"

def get_data():
    with open('./data.json', 'r') as f:
        data = json.load(f, strict=False)
    return data


def check_product(checking_product: dict) -> bool:
    return list(checking_product.keys()) == [
        "Name",
        "Category",
        "ImageLink",
        "EAN",
        "Number pieces in packaging",
        "Number of cases on the pallet",
        "Number of pieces on the pallet",
    ] or list(product.keys()) == [
        "Name",
        "Category",
        "ImageLink",
        "EAN",
        "Number of pieces in the package",
        "Number of packages on a pallet",
        "Number of pieces on a pallet",
    ]
    
    
def check_issued_data(issued_product: dict) -> bool:
    required_keys = ["Name", "Category", "ImageLink"]

    return all(required_key in issued_product for required_key in required_keys)


def default_ean(checking_product: dict) -> str:
    if "EAN" not in checking_product:
        checking_product["EAN"] = None
    return checking_product
    
def insert_data(data, is_issued=False):
    product = default_ean(data)
    
    category_id = CATEOGRIES_IDS[product["Category"]]
    
    if is_issued:
        c.execute(QUERY, (product["Name"], product["ImageLink"], product["EAN"], None, None, None, False, False, category_id))
    elif "Number pieces in packaging" in product:
        c.execute(QUERY, (product["Name"], product["ImageLink"], product["EAN"], product["Number pieces in packaging"], product["Number of cases on the pallet"], product["Number of pieces on the pallet"], True, False, category_id))
    elif "Number of pieces in the package" in product:
        c.execute(QUERY, (product["Name"], product["ImageLink"], product["EAN"], product["Number of pieces in the package"], product["Number of packages on a pallet"], product["Number of pieces on a pallet"], True, False, category_id))
    

try:
    conn = psycopg2.connect(dbname=DB_NAME, user=USER, password=PASSWORD, host=HOST, port=PORT)
    c = conn.cursor()
except Exception as e:
    print("Something went wrong with connection", e)
    exit()

data = get_data()

# List for products with issues
issued = []

for product in data:
    if check_product(product):
        insert_data(product)
    else:
        issued.append(product)        

for issued_product in issued:
    if check_issued_data(issued_product):
        insert_data(issued_product, is_issued=True)
    else:
        print(issued_product)
