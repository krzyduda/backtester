# HACKATON:

## environment set up:
https://www.quantstart.com/articles/Installing-a-Desktop-Algorithmic-Trading-Research-Environment-using-Ubuntu-Linux-and-Python

VB, ubuntu with conda 3

create quant envirnment

problem with ipython and qt - can solve it later


## securities master database for algo trading:

Install mysql
create db and user
run sql scripts to populate db tables

py script to import metadata

### script to load eod historic prices

problem:
how to get free data

sources:
stooq - limit, not usable
alphavantage - daily and minute limits
quandl - free data stops in 2018 (however, it's good for research)

### querying

loading data from sql to pandas data frame - works fine


TODO:

improve performance of load
refactor:

* extract fetching data from api (different sources)
* single way of inserting data to sql


## backtesting

