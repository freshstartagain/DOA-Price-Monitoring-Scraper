import tabula
import datetime
import csv


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


def main():
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

    # for key, product in products.items():
    #     print(f"{key} : {product}")

    with open("output.csv", newline="") as csvfile:
        reader = csv.reader(csvfile, delimiter=",")
        for row in reader:
            try:
                product_name = row[0]
                specifications = row[2]
                prices = row[3].split()
                prevailing_price = prices[0] if prices[0] not in ['#N/A', '#DIV/0', '#DIV/0!'] else ''
                low_price = prices[1] if prices[1] not in ['#N/A', '#DIV/0', '#DIV/0!'] else ''
                high_price = prices[2] if prices[2] not in ['#N/A', '#DIV/0', '#DIV/0!'] else ''
                average_price = prices[3] if prices[3] not in ['#N/A', '#DIV/0', '#DIV/0!'] else ''

                print(
                    f"Product Name:{product_name}, Specifications:{specifications}, Prevailing Price:{prevailing_price}, Low Price:{low_price}, High Price: {high_price}, Average Price: {average_price}"
                )
            except IndexError:
                pass


if __name__ == "__main__":
    main()
