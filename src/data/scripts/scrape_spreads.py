from src.data import scrapers
from src.data import Transfer

rows = scrapers.spreads.run()
Transfer.insert('spreads', rows, at_once=False)
