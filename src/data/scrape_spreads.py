from bs4 import BeautifulSoup
import requests
import datetime
import requests
import Transfer
import Scrapers

url = "https://www.vegasinsider.com/college-basketball/odds/las-vegas/"
Transfer.scrape_insert(Scrapers.scrape_spreads, url, 'spreads')
