#!/usr/bin/python
# -*- coding: utf-8 -*-

import datetime
import lxml.html
import requests
import mysql.connector

from math import ceil


def obtain_parse_wiki_snp500():
  """Download and parse the Wikipedia list of S&P500 
  constituents using requests and libxml.

  Returns a list of tuples for to add to MySQL."""

  # Stores the current time, for the created_at record
  now = datetime.datetime.utcnow()

  # Use libxml to download the list of S&P500 companies and obtain the symbol table
  request = requests.get('http://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
  page = lxml.html.fromstring(request.text)
  symbolslist = page.xpath('//table[1]/tbody/tr')[1:]

  # Obtain the symbol information for each row in the S&P500 constituent table
  symbols = []
  for symbol in symbolslist:
    tds = symbol.getchildren()
    sd = {'ticker': tds[0].getchildren()[0].text,
        'name': tds[1].getchildren()[0].text,
        'sector': tds[3].text}
    # Create a tuple (for the DB format) and append to the grand list
    symbols.append( (sd['ticker'], 'stock', sd['name'], 
      sd['sector'], 'USD', now, now) )
  return symbols

def insert_snp500_symbols(symbols):
  """Insert the S&P500 symbols into the MySQL database."""

  # Connect to the MySQL instance
  db_host = 'localhost'
  db_user = 'sec_user'
  db_pass = 'password'
  db_name = 'securities_master'
  con = mysql.connector.connect(host=db_host, user=db_user, password=db_pass, database=db_name)

  # Create the insert strings
  column_str = "ticker, instrument, name, sector, currency, created_date, last_updated_date"
  insert_str = ("%s, " * 7)[:-2]
  final_str = "INSERT INTO symbol (%s) VALUES (%s)" % (column_str, insert_str)
  print(final_str, len(symbols))

  # Using the MySQL connection, carry out an INSERT INTO for every symbol
  cur = con.cursor()

  cur.executemany(final_str, symbols)
  con.commit()

  cur.close()
  con.close()

if __name__ == "__main__":
  symbols = obtain_parse_wiki_snp500()
  insert_snp500_symbols(symbols)