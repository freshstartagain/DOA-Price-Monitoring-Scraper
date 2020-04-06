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
    with open("output.csv", newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=",")
        for row in reader:
            print(row)
    


if __name__ == "__main__":
    main()
