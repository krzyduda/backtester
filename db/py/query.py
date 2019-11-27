#!/usr/bin/python
# -*- coding: utf-8 -*-

import pandas as pd
import pandas.io.sql as psql
import mysql.connector


# Connect to the MySQL instance
db_host = 'localhost'
db_user = 'sec_user'
db_pass = 'password'
db_name = 'securities_master'
con = mysql.connector.connect(host=db_host, user=db_user, password=db_pass, database=db_name)

# Select all of the historic Google adjusted close data
sql = """SELECT dp.price_date, dp.adj_close_price
         FROM symbol AS sym
         INNER JOIN daily_price AS dp
         ON dp.symbol_id = sym.id
         WHERE sym.ticker = 'MMM'
         ORDER BY dp.price_date ASC;"""

# Create a pandas dataframe from the SQL query
goog = psql.read_sql_query(sql, con=con, index_col='price_date')    

# Output the dataframe tail
print (goog.tail())