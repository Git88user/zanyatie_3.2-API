import requests
import datetime

url_conf = {
    "table_name": "rates",
    "rate_base": "BTC",
    "rate_target": "RUB",
    "url_base": "https://api.exchangerate.host/timeseries?start_date=2023-06-01&end_date=2023-06-30"
}

url = url_conf['url_base']

try:
    response = requests.get(url, params={'base': url_conf["rate_base"], 'symbols': url_conf["rate_target"]})
except Exception as err:
    print(f'Error occured: {err}')

data = response.json()

print(data)

import psycopg2
from psycopg2 import Error

host = 'localhost'
port = '5432'
username = 'postgres'
password = 'test'
database = 'postgres'

try:
    conn = psycopg2.connect(
        host=host,
        port=port,
        user=username,
        password=password,
        database=database
    )
    cur = conn.cursor()
    print("Successfully connected to the database")

    create_table_query = '''CREATE TABLE if not exist BitcKurs(id PRIMARY KEY, date DATE NOT NULL, btc FLOAT NOT
    NULL, rub FLOAT NOT NULL)'''

    i = 0
    i = i + 1

    insert_table_query = '''INSERT INTO BitcoinKurs(id, date, btc, rub) VALUES
                            (%s, %s, %s, %s)''', (i, url_conf["rates"], url_conf["rate_base"], url_conf["rate_target"])

    select_table_query = ('''SELECT
                        date as date_MaxKurs WHERE rub = (SELECT MAX(rub) FROM BitcKurs),
                        date as date_MinKurs WHERE rub = (SELECT MIN(rub) FROM BitcKurs),
                        MAX(rub) as MaxKurs_rub,
                        MIN(rub) as MinKurs_rub,
                        AVG(rub) as AvgKurs_rub,
                        rub as rub_last WHERE date = 2023-06-30
                        FROM BitcKurs''')

    cur.execute(create_table_query, insert_table_query, select_table_query)
    vit_BitcKurs = cur.fetchall()

    print("Data on the exchange rate of BTC in RUB:")
    for row in vit_BitcKurs:
        print('date_MaxKurs = ', row[0])
        print('date_MinKurs = ', row[1])
        print('MaxKurs_rub = ', row[2])
        print('MinKurs_rub = ', row[3])
        print('AvgKurs_rub = ', row[4])
        print('rub_last = ', row[5])

    conn.commit()

except (Exception, Error) as error:
    print('Oshibka pri rabote s PostgreSQL', error)
finally:
    if conn:
        cur.close()
        conn.close()
        print('Soedinenie s PostgreSQL zakrito')