import datetime
import csv
import os

import tabula
from bs4 import BeautifulSoup
import requests


class PriceMonitoring:
    def __init__(self, pdf):
        self.pdf = pdf

    @staticmethod
    def csv_file(self):
        try:
            tabula.convert_into(
                self.pdf,
                f"price_monitoring_{datetime.date.today()}",
                output_format="csv",
                pages="1",
            )
            return True
        except:
            return False

    def clean_csv_file(self):
        pass


def clean_csv(filename, date):
    products = {
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

    with open(filename, newline="") as csvfile:
        os.remove(filename)
        reader = csv.reader(csvfile, delimiter=",")
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
                    prevailing_price = (
                        prices[0]
                        if prices[0] not in ["#N/A", "#DIV/0", "#DIV/0!"]
                        else ""
                    )
                    low_price = (
                        prices[1]
                        if prices[1] not in ["#N/A", "#DIV/0", "#DIV/0!"]
                        else ""
                    )
                    high_price = (
                        prices[2]
                        if prices[2] not in ["#N/A", "#DIV/0", "#DIV/0!"]
                        else ""
                    )
                    average_price = (
                        prices[3]
                        if prices[3] not in ["#N/A", "#DIV/0", "#DIV/0!"]
                        else ""
                    )

                    for key, product in products.items():
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
                                    date[0],
                                    date[1],
                                    date[2],
                                ]
                            )
                            # print(
                            #     f"Product Name:{product_name}, Specifications:{specifications}, Prevailing Price:{prevailing_price}, Low Price:{low_price}, High Price: {high_price}, Average Price: {average_price}, Key: {key}"
                            # )
                except IndexError:
                    pass


def scraper():
    response = requests.get("http://www.da.gov.ph/price-monitoring/").text
    soup = BeautifulSoup(response, "html.parser")
    pdf_links = soup.find("tbody").find_all("a")

    for pdf_link in pdf_links:
        href = pdf_link["href"]
        if "2020.pdf" in href.split("-"):
            date = (
                href.split("Monitoring")[1]
                .replace(".pdf", "")
                .replace("_", "")
                .lstrip("-")
            )
            filename = f"product_prices_{date}.csv"
            print(href, filename)

            pdf_to_csv(href, filename, date)


def pdf_to_csv(pdf_url, filename, date):
    tabula.convert_into(
        pdf_url, filename, output_format="csv", pages="1",
    )

    clean_csv(filename, date.split("-"))


def main():
    scraper()


if __name__ == "__main__":
    main()
