from src.data import scrapers
from src.data import transfer

rows = scrapers.spreads.run()
transfer.insert('spreads', rows, at_once=False)
