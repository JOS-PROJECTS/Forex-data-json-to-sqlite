import sqlite3
import json
import datetime




conn = sqlite3.connect('forex_currency__data.db')

conn.execute("DROP TABLE IF EXISTS forex__table;")
conn.execute("CREATE TABLE forex__table (time_year int,time_month int,time_day int,time_hour int,time_minute int, AUDJPY float,AUDNZD float,AUDUSD float,CADJPY float,EURJPY float,EURUSD float,GBPJPY float,USDJPY float);")


with open('realtime_forex.json', 'r') as json_file:
    data = json.load(json_file)
    for time_stamp,currency_data in data.items():
        full_date=datetime.datetime.strptime(time_stamp,'%a %b %d %H:%M:%S %Y')
        time_year=full_date.year
        time_month=full_date.month
        time_day=full_date.day
        time_hour=full_date.hour
        time_minute=full_date.minute
        conn.execute("INSERT INTO forex__table (time_year, time_month, time_day, time_hour, time_minute, AUDJPY, AUDNZD, AUDUSD, CADJPY, EURJPY, EURUSD, GBPJPY, USDJPY) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (time_year, time_month, time_day, time_hour, time_minute, currency_data['AUDJPY'],currency_data['AUDNZD'],currency_data['AUDUSD'],currency_data['CADJPY'],currency_data['EURJPY'],currency_data['EURUSD'],currency_data['GBPJPY'],currency_data['USDJPY']))

conn.commit()

#conn.execute("SELECT * FROM forex__table;")

conn.close()
