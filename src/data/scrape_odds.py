import Transfer
import Scrapers

url = "http://www.vegasinsider.com/college-basketball/odds/las-vegas/money/"
Transfer.scrape_insert(Scrapers.scrape_odds, url, 'odds')
