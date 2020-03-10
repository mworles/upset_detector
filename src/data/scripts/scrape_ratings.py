from src.data import scrapers
from src.data import Transfer

rows = scrapers.ratings.run()
Transfer.insert('ratings', rows, at_once=False)
