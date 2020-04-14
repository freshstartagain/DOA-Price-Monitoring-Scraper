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
        
    
    def database_process(self, product_data):
        db_connection = mysql.connect(host = self.DB_HOST, user = self.DB_USERNAME, password = self.DB_PASSWORD)
        db_cursor = db_connection.cursor()

        try:
            db_cursor.execute(self.CREATE_DATABASE_QUERY)
            db_cursor.execute(self.USE_DATABASE_QUERY)
            db_cursor.execute(self.CREATE_SPECIFICATION_TABLE_QUERY)
            db_cursor.execute(self.CREATE_TYPE_TABLE_QUERY)
            db_cursor.execute(self.CREATE_PRODUCT_TABLE_QUERY)
        except mysql.Error as error:
            print(f"Something went wrong: {error}")
            

    def __create_price(self, price):
        return price if price not in self.INVALID_PRICES else ""

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
        filename = f"product_prices_{publish_date}.csv"

        tabula.convert_into(pdf_link, filename, output_format="csv", pages="1")

        with open(filename, newline="") as csvfile:
            os.remove(filename)

            reader = csv.reader(csvfile, delimiter=self.CSV_DELIMITER)
            with open(filename, "w", newline="") as file:
                writer = csv.writer(file)
                writer.writerow(
                    [
                        "product_name",
                        "specifications",
                        "prevailing_price",
                        "low_price",
                        "high_price",
                        "average_price",
                        "type",
                        "month",
                        "date",
                        "year",
                    ]
                )
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
                                writer.writerow(
                                    [
                                        product_name,
                                        specifications,
                                        prevailing_price,
                                        low_price,
                                        high_price,
                                        average_price,
                                        key,
                                        month,
                                        day,
                                        year,
                                    ]
                                )
                                # print(
                                #     f"Product Name:{product_name}, Specifications:{specifications}, Prevailing Price:{prevailing_price}, Low Price:{low_price}, High Price: {high_price}, Average Price: {average_price}, Key: {key}, Month: {month}, Date: {date}, Year: {year}"
                                # )
                    except IndexError:
                        pass

    def generate(self):
        for link in self.doa_pdf_links():
            self.convert_pdf_to_csv(link)


if __name__ == "__main__":
    price = PriceMonitoring("http://www.da.gov.ph/price-monitoring/")
    # price.generate()
    price.database_process()
    

