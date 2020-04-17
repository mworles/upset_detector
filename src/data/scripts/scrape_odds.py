from src.data import scrapers
from src.data import transfer

rows = scrapers.odds.run()
transfer.insert('odds', rows, at_once=False)
