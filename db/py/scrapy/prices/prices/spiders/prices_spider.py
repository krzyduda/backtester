import scrapy
import mysql.connector

def obtain_list_of_db_tickers(con):
    """Obtains a list of the ticker symbols in the database."""
    cur = con.cursor()
    cur.execute("SELECT id, ticker FROM symbol")
    data = cur.fetchall()
    cur.close()
    return [(d[0], d[1]) for d in data]


class QuotesSpider(scrapy.Spider):
    name = "prices"

    def start_requests(self):
        # Obtain a database connection to the MySQL instance
        db_host = 'localhost'
        db_user = 'sec_user'
        db_pass = 'password'
        db_name = 'securities_master'
        con = mysql.connector.connect(host=db_host, user=db_user, password=db_pass, database=db_name)

        # Loop over the tickers and insert the daily historical
        # data into the database
        tickers = obtain_list_of_db_tickers(con)

        for ticker in tickers:
            url = f'https://www.quandl.com/api/v3/datasets/WIKI/{ticker[1]}.csv?api_key=csxSr2B3orHa6r2fYSs4'
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        filename = response.url.split('/')[-1].split('?')[0]
        with open(filename, 'wb') as f:
            f.write(response.body)
        self.log('Saved file %s' % filename)