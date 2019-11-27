#!/usr/bin/python
# -*- coding: utf-8 -*-

import datetime
import mysql.connector
import requests


def obtain_list_of_db_tickers(con):
  """Obtains a list of the ticker symbols in the database."""
  cur = con.cursor()
  cur.execute("SELECT id, ticker FROM symbol")
  data = cur.fetchall()
  cur.close()
  return [(d[0], d[1]) for d in data]

def get_daily_historic_data_from_quandl(ticker,
                      start_date=(2000,1,1),
                      end_date=datetime.date.today().timetuple()[0:3]):
    """Obtains data returns and a list of tuples.

  ticker: ticker symbol, e.g. "GOOG" for Google, Inc.
  start_date: Start date in (YYYY, M, D) format
  end_date: End date in (YYYY, M, D) format"""

    # Construct the URL with the correct integer query parameters
    url = "https://www.quandl.com/api/v3/datasets/WIKI/%s.csv?start_date=2000-01-01&api_key=csxSr2B3orHa6r2fYSs4" % \
      (ticker)
    
    # Try connecting and obtaining the data
    # On failure, print an error message.
    try:
        data = requests.get(url).text.splitlines()[1:] # Ignore the header
        prices = []
        for y in data:
          p = y.strip().split(',')
          prices.append( (datetime.datetime.strptime(p[0], '%Y-%m-%d'),
                  p[1], p[2], p[3], p[4], p[5], p[11]) )
    except Exception as e:
        print("Could not download data: %s" % e)
    return prices

def get_daily_historic_data_from_alpha(ticker,
                      start_date=(2000,1,1),
                      end_date=datetime.date.today().timetuple()[0:3]):
    """Obtains data returns and a list of tuples.

  ticker: ticker symbol, e.g. "GOOG" for Google, Inc.
  start_date: Start date in (YYYY, M, D) format
  end_date: End date in (YYYY, M, D) format"""

    # Construct the URL with the correct integer query parameters
    url = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol=%s&outputsize=full&apikey=HD8X3DOGD1IBP798&datatype=csv" % \
      (ticker)
    
    # Try connecting and obtaining the data
    # On failure, print an error message.
    try:
        data = requests.get(url).text.splitlines()[1:] # Ignore the header
        prices = []
        for y in data:
          p = y.strip().split(',')
          prices.append( (datetime.datetime.strptime(p[0], '%Y-%m-%d'),
                  p[1], p[2], p[3], p[4], p[5], p[6]) )
    except Exception as e:
        print("Could not download data: %s" % e)
    return prices

def get_daily_historic_data_from_stooq(ticker,
                      start_date=(2000,1,1),
                      end_date=datetime.date.today().timetuple()[0:3]):
    """Obtains data returns and a list of tuples.

  ticker: ticker symbol, e.g. "GOOG" for Google, Inc.
  start_date: Start date in (YYYY, M, D) format
  end_date: End date in (YYYY, M, D) format"""

    # Construct the URL with the correct integer query parameters
    url = "https://stooq.com/q/d/l/?s=%s.us&i=d" % \
      (ticker)
    
    # Try connecting and obtaining the data
    # On failure, print an error message.
    try:
        data = requests.get(url).text.splitlines()[1:] # Ignore the header
        prices = []
        for y in data:
          p = y.strip().split(',')
          prices.append( (datetime.datetime.strptime(p[0], '%Y-%m-%d'),
                  p[1], p[2], p[3], p[4], p[5], p[4]) )
    except Exception as e:
        print("Could not download data: %s" % e)
    return prices

def insert_daily_data_into_db(con, data_vendor_id, symbol_id, daily_data):
  """Takes a list of tuples of daily data and adds it to the
  MySQL database. Appends the vendor ID and symbol ID to the data.

  daily_data: List of tuples of the OHLC data (with 
  adj_close and volume)"""
  
  # Create the time now
  now = datetime.datetime.utcnow()

  # Amend the data to include the vendor ID and symbol ID
  daily_data = [(data_vendor_id, symbol_id, d[0], now, now,
    d[1], d[2], d[3], d[4], d[5], d[6]) for d in daily_data]

  # Create the insert strings
  column_str = """data_vendor_id, symbol_id, price_date, created_date, 
          last_updated_date, open_price, high_price, low_price, 
          close_price, volume, adj_close_price"""
  insert_str = ("%s, " * 11)[:-2]
  final_str = "INSERT INTO daily_price (%s) VALUES (%s)" % (column_str, insert_str)

  # Using the MySQL connection, carry out an INSERT INTO for every symbol
  cur = con.cursor()
  cur.executemany(final_str, daily_data)
  con.commit()
  cur.close()

if __name__ == "__main__":

  # Obtain a database connection to the MySQL instance
  db_host = 'localhost'
  db_user = 'sec_user'
  db_pass = 'password'
  db_name = 'securities_master'
  con = mysql.connector.connect(host=db_host, user=db_user, password=db_pass, database=db_name)

  # Loop over the tickers and insert the daily historical
  # data into the database
  tickers = obtain_list_of_db_tickers(con)

  for t in tickers:
    print("Adding data for %s" % t[1])
    yf_data = get_daily_historic_data_from_quandl(t[1])
    insert_daily_data_into_db(con, '1', t[0], yf_data)

  con.commit()
  con.close()