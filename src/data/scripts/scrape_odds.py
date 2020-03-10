from src.data import scrapers
from src.data import Transfer

rows = scrapers.odds.run()
Transfer.insert('odds', rows, at_once=False)
