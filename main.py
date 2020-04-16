import datetime
import csv
import os
from pathlib import Path


import tabula
from bs4 import BeautifulSoup
import requests
import mysql.connector as mysql
from dotenv import load_dotenv


ENV_PATH = Path('.') / '.env'
load_dotenv(dotenv_path=ENV_PATH)  


class PriceMonitoring:
    PRODUCTS = {
        "nfa_rice": ["NFA (Well milled)",],
        "imported_commercial_rice": [
            "Special (Blue tag)",
            "Premium (Yellow tag)",
            "Well milled (White tag)",
            "Regular milled (White tag)",
            "Special (Blue tag)",
            "Premium (Yellow tag)",
            "Well milled (White tag)",
            "Regular milled (White tag)",
        ],
        "local_commercial_rice": [
            "Special (Blue tag)",
            "Premium (Yellow tag)",
            "Well milled (White tag)",
            "Regular milled (White tag)",
        ],
        "fish": [
            "Bangus",
            "Tilapia",
            "Galunggong (Local)",
            "Galunggong (Imported)",
            "Alumahan",
        ],
        "livestock_poultry_products": [
            "Beef Rump",
            "Beef Brisket",
            "Pork Ham (Kasim)",
            "Pork Belly (Liempo)",
            "Whole Chicken",
            "Chicken Egg",
        ],
        "lowland_vegetables": [
            "Ampalaya",
            "Sitao",
            "Pechay (Native)",
            "Squash",
            "Eggplant",
            "Tomato",
        ],
        "highland_vegetables": [
            "Cabbage (Scorpio)",
            "Carrots",
            "Habitchuelas (Baguio beans)",
            "White Potato",
            "Pechay (Baguio)",
            "Chayote",
        ],
        "spices": [
            "Red Onion",
            "Red Onion (Imported)",
            "White Onion",
            "White Onion (Imported)",
            "Garlic (Imported)",
            "Garlic (Native)",
            "Ginger",
            "Chilli (Labuyo)",
        ],
        "fruits": [
            "Calamansi",
            "Banana (Lakatan)",
            "Banana (Latundan)",
            "Papaya",
            "Mango (Carabao)",
        ],
        "other_basic_commodities": [
            "Sugar (Refined)",
            "Sugar (Washed)",
            "Sugar (Brown)",
            "Cooking oil (Palm)",
            "Cooking oil (Palm)",
        ],
    }

    YEAR = datetime.datetime.now().year
    PDF_DELIMITER = "-"
    CSV_DELIMITER = ","
    INVALID_PRICES = ["#N/A", "#DIV/0", "#DIV/0!", "NONE"]
    DB_HOST = os.getenv('HOST')
    DB_USERNAME = os.getenv('USER')
    DB_PASSWORD = os.getenv('PASSWORD')
    DB_NAME = os.getenv('NAME')
    CREATE_DATABASE_QUERY = "CREATE DATABASE IF NOT EXISTS doa_products;"
    USE_DATABASE_QUERY = f"USE doa_products;"
    CREATE_SPECIFICATION_TABLE_QUERY = "CREATE TABLE IF NOT EXISTS specification(id INT AUTO_INCREMENT, name VARCHAR(200), PRIMARY KEY(id));"
    CREATE_TYPE_TABLE_QUERY = "CREATE TABLE IF NOT EXISTS type(id INT AUTO_INCREMENT, name VARCHAR(200), PRIMARY KEY(id));"
    CREATE_PRODUCT_TABLE_QUERY = "CREATE TABLE  IF NOT EXISTS product(id INT AUTO_INCREMENT, name VARCHAR(200), prevailing_price DECIMAL(10,2), low_price DECIMAL(10,2), high_price DECIMAL(10,2), average_price DECIMAL(10,2), issue_date DATE, specification_id INT, type_id INT, PRIMARY KEY(id), FOREIGN KEY (specification_id) REFERENCES specification(id), FOREIGN KEY (type_id) REFERENCES type(id));"

    

    def __init__(self, doa_url):
        self.doa_url = doa_url
    
    def __get_table_column_id(self, table, column_value, connection, cursor):
        cursor.execute(f"SELECT * FROM {table} WHERE name='{column_value}';")
        column = cursor.fetchone()
        if column:
           return column['id']
        else:
            cursor.execute(f"INSERT INTO {table} (name) VALUES ('{column_value}');")
            connection.commit()
            cursor.execute(f"SELECT * FROM {table} WHERE name='{column_value}';")
            column = cursor.fetchone()
            return column['id']

    def __seed_product_table(self, product, connection, cursor):
        name = product['name']
        prevailing_price = product['prevailing_price']
        low_price = product['low_price']
        high_price = product['high_price']
        average_price = product['average_price']
        issue_date = product['issue_date']
        specification_id = product['specification_id']
        type_id = product['type_id']

        cursor.execute(f"SELECT * FROM product WHERE name='{name}' AND issue_date='{issue_date}';")
        product = cursor.fetchone()
        if product:
            pass
        else:
            cursor.execute(f"INSERT INTO product (name, prevailing_price, low_price, high_price, average_price, issue_date, specification_id, type_id) VALUES ('{name}', '{prevailing_price}', '{low_price}', '{high_price}', '{average_price}', '{issue_date}', '{specification_id}', '{type_id}');")
            connection.commit()
            print(f"Done adding {name}:{issue_date}")

    
    def __database_process(self, product):
        db_connection = mysql.connect(host = self.DB_HOST, user = self.DB_USERNAME, password = self.DB_PASSWORD)
        db_cursor = db_connection.cursor(dictionary=True)
        
        try:
            db_cursor.execute(self.CREATE_DATABASE_QUERY)
            db_cursor.execute(self.USE_DATABASE_QUERY)
            db_cursor.execute(self.CREATE_SPECIFICATION_TABLE_QUERY)
            db_cursor.execute(self.CREATE_TYPE_TABLE_QUERY)
            db_cursor.execute(self.CREATE_PRODUCT_TABLE_QUERY)
        except mysql.Error as error:
            print(f"Something went wrong: {error}")
        
        specification_id = self.__get_table_column_id("specification", product['specification'], db_connection, db_cursor)
        type_id = self.__get_table_column_id("type", product['type'], db_connection, db_cursor)

        final_product_data = {
            "name":product["name"],
            "prevailing_price":product["prevailing_price"],
            "low_price":product["low_price"],
            "high_price":product["high_price"],
            "average_price":product["average_price"],
            "issue_date":product["date"],
            "specification_id":specification_id,
            "type_id":type_id,
        }

        self.__seed_product_table(final_product_data, db_connection, db_cursor)

    def __create_price(self, price):
        return price if price not in self.INVALID_PRICES else 0

    def doa_pdf_links(self):
        response = requests.get(self.doa_url).text
        soup = BeautifulSoup(response, "html.parser")
        anchor_tags = soup.find("tbody").find_all("a")

        return [
            anchor["href"]
            for anchor in anchor_tags
            if f"{self.YEAR}.pdf" in anchor["href"].split(self.PDF_DELIMITER)
        ]

    def convert_pdf_to_csv(self, pdf_link):
        publish_date = (
            pdf_link.split("Monitoring")[1]
            .replace(".pdf", "")
            .replace("_", "")
            .lstrip("-")
        )

        month, day, year = tuple(publish_date.split("-"))

        if len(month) == 2:
            raw_date = datetime.datetime.strptime(f"{day} {month}, {year}", '%d %m, %Y')
            date = raw_date.strftime('%Y-%m-%d')
        elif len(month) == 3:
            raw_date = datetime.datetime.strptime(f"{day} {month}, {year}", '%d %b, %Y')
            date = raw_date.strftime('%Y-%m-%d')
        elif len(month) > 3:
            raw_date = datetime.datetime.strptime(f"{day} {month}, {year}", '%d %B, %Y')
            date = raw_date.strftime('%Y-%m-%d')

        filename = f"product_prices_{publish_date}.csv"

        tabula.convert_into(pdf_link, filename, output_format="csv", pages="1")

        with open(filename, newline="") as csvfile:
            os.remove(filename)
            reader = csv.reader(csvfile, delimiter=self.CSV_DELIMITER)

            for row in reader:
                try:
                    product_name = row[0]
                    specifications = row[2]
                    prices = row[3].split()
                    prevailing_price = self.__create_price(prices[0]) 
                    low_price = self.__create_price(prices[1])
                    high_price = self.__create_price(prices[2])
                    average_price = self.__create_price(prices[3])

                    for key, product in self.PRODUCTS.items():
                        if product_name in product:  
                            self.__database_process({
                                'name':product_name,
                                'specification':specifications,
                                'prevailing_price':prevailing_price,
                                'low_price':low_price,
                                'high_price':high_price,
                                'average_price':average_price,
                                'type':key,
                                'date':date,
                            })
                except IndexError:
                    pass

    def generate(self):
        for link in self.doa_pdf_links():
            self.convert_pdf_to_csv(link)


if __name__ == "__main__":
    price = PriceMonitoring("http://www.da.gov.ph/price-monitoring/")
    price.generate()
    

