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


#CREATE & INSERT MONTHLY TABLES
for mnth in range(1,13):
    mn_table_name='month'+str(mnth)+'table'
    conn.execute(f"DROP TABLE IF EXISTS {mn_table_name};")
    conn.execute(f"CREATE TABLE {mn_table_name}(dAy int, Hour int, Minute int, AUDJPY float, AUDNZD float, AUDUSD float, CADJPY float, EURJPY float, EURUSD float, GBPJPY float, USDJPY float);")
    conn.execute(f"insert into {mn_table_name} select time_day, time_hour, time_minute, AUDJPY, AUDNZD, AUDUSD, CADJPY, EURJPY, EURUSD, GBPJPY, USDJPY from forex__table where time_month={mnth};")


#CREATE & INSERT MONTHLY AVERAGE TABLES
for mon in range(1,13):
    table_name='month_avg'+str(mon)+'table'
    #print(table_name)
    conn.execute(f"DROP TABLE IF EXISTS {table_name};")
    conn.execute(f"CREATE TABLE {table_name}(AUDJPY float, AUDNZD float, AUDUSD float, CADJPY float, EURJPY float, EURUSD float, GBPJPY float, USDJPY float);")
    conn.execute(f"insert into {table_name} select avg(AUDJPY), avg(AUDNZD), avg(AUDUSD), avg(CADJPY), avg(EURJPY), avg(EURUSD), avg(GBPJPY), avg(USDJPY) from forex__table where time_month={mon};") 

#CREATE & INSERT DAILYperMONTH TABLES
for mn in range(1,13):
    for dy in range(1,32):
        monthly_table_name='month'+str(mn)+'table'
        day_mn_table_name='month'+str(mn)+'day'+str(dy)+'table'
        conn.execute(f"DROP TABLE IF EXISTS {day_mn_table_name};")
        conn.execute(f"CREATE TABLE {day_mn_table_name}(dAy int, Hour int, Minute int, AUDJPY float, AUDNZD float, AUDUSD float, CADJPY float, EURJPY float, EURUSD float, GBPJPY float, USDJPY float);")
        conn.execute(f"insert into {day_mn_table_name} select dAy, Hour, Minute, AUDJPY, AUDNZD, AUDUSD, CADJPY, EURJPY, EURUSD, GBPJPY, USDJPY from {monthly_table_name} where dAy={dy};")


#CREATE & INSERT avgDAILYperMONTH TABLEs
for mn in range(1,13):
    for dy in range(1,32):
        monthly_table_name='month'+str(mn)+'table'
        avg_day_mn_table_name='month'+str(mn)+'avg_day'+str(dy)+'table'
        conn.execute(f"DROP TABLE IF EXISTS {avg_day_mn_table_name};")
        conn.execute(f"CREATE TABLE {avg_day_mn_table_name}(dAy int, AUDJPY float, AUDNZD float, AUDUSD float, CADJPY float, EURJPY float, EURUSD float, GBPJPY float, USDJPY float);")
        conn.execute(f"insert into {avg_day_mn_table_name} select dAy, avg(AUDJPY),avg(AUDNZD), avg(AUDUSD), avg(CADJPY), avg(EURJPY), avg(EURUSD), avg(GBPJPY), avg(USDJPY) from {monthly_table_name} where dAy={dy};")

conn.commit()

#conn.execute("SELECT * FROM forex__table;")

conn.close()

