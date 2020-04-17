from src.data import scrapers
from src.data import transfer

rows = scrapers.ratings.run()
transfer.insert('ratings', rows, at_once=False)
