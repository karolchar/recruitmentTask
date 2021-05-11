#############################################
# To export table product into excel file   #
# write python3 main.py --export in console #
#############################################
import requests
import mysql.connector
from mysql.connector import Error
import pandas.io.sql as sql
import argparse
import logging
from requests import exceptions as req_error

#define logging module
logging.basicConfig(filename='errors.log', filemode='w')
#define parser for exporting table product to excel
parser = argparse.ArgumentParser()
parser.add_argument("--export", action="store_true")
args = parser.parse_args()

#get values USD and EUR values from NBP api
#if status code != 200 then stop program
try:
    response_usd = requests.get("http://api.nbp.pl/api/exchangerates/rates/a/usd/?format=json")
    response_eur = requests.get("http://api.nbp.pl/api/exchangerates/rates/a/eur/?format=json")
    response_usd.raise_for_status()
    response_eur.raise_for_status()
except req_error.HTTPError as e:
    logging.error(e)
    raise SystemExit(e)

#function to retreive currency value
#param argument is api response in json format
def get_currency_from_api(response):
    for key, value in response.items():
        if isinstance(value, list):
            # unpack currency value
            currency_value = [x[y] for x in value for y in x if y == 'mid']
            return currency_value


class Connection:
    @staticmethod
    def establish_connection():
        try:
            connection = mysql.connector.connect(
                host='127.0.0.1',
                port='3306',
                user='root',
                password='root',
                database='mydb',
            )
        except Error as err:
            logging.info(err)
        else:
            return connection


class UpdateDatabase(Connection):
    def __init__(self):
        self.connection = self.establish_connection()
        self.cursor = self.connection.cursor()

    def update(self, value_usd, value_eur):
        self.cursor.execute("SELECT UnitPrice from product")

        #get all records of column UnitPrice
        data = self.cursor.fetchall()
        update_column = """UPDATE product SET UnitPriceUSD = %s, UnitPriceEUR = %s WHERE UnitPrice=%s;"""

        #calculate unit_price_usd and unit_price_eur using unit_price and currency value for each row
        #then update columns unit_price_usd and unit_price_eur
        for unit_price in data:
            unit_price_usd = float(unit_price[0]) * value_usd[0]
            unit_price_eur = float(unit_price[0]) * value_eur[0]
            self.cursor.execute(update_column, (round(unit_price_usd, 2), round(unit_price_eur, 2), unit_price[0]))
            self.connection.commit()

        #checks if flag was used in terminal with flag --export if True exports product table into excel file
        if args.export:
            excel = sql.read_sql('select * from product', self.connection)

            excel.to_excel('products.xlsx', columns=['ProductID', 'DepartmentID', 'Category', 'IDSKU', 'ProductName',
                                                     'Quantity', 'UnitPrice', 'UnitPriceUSD', 'UnitPriceEUR', 'Ranking',
                                                     'ProductDesc', 'UnitsInStock', 'UnitsInOrder'], index=False)
        self.cursor.close()
        self.connection.close()


if __name__ == '__main__':
    value_usd_api = get_currency_from_api(response_usd.json())
    value_eur_api = get_currency_from_api(response_eur.json())
    UpdateDatabase().update(value_usd_api, value_eur_api)
