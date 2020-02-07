import Transfer
import Scrapers

url = 'http://kenpom.com/index.php'
Transfer.scrape_insert(Scrapers.team_ratings, url, 'ratings')
