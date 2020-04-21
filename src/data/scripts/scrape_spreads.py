from src.data import scrapers
from src.data import transfer

rows = scrapers.spreads.run()
dba = transfer.DBAssist()

dba.insert('spreads', rows, at_once=False)
