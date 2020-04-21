from src.data import scrapers
from src.data import transfer

rows = scrapers.odds.run()
dba = Transfer.DBAssist()
dba.insert('odds', rows, at_once=False)
