from src.data import scrapers
from src.data.transfer import DBAssist

rows = scrapers.spreads.run()
dba = DBAssist()
dba.insert_rows('spreads', rows, at_once=False)
dba.close()
