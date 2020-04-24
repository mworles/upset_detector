from src.data import scrapers
from src.data.transfer import DBAssist

rows = scrapers.odds.run()

dba = DBAssist()
dba.insert_rows('odds', rows, at_once=False)
dba.close()
