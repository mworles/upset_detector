from src.data import scrapers
from src.data import transfer

rows = scrapers.ratings.run()
dba = Transfer.DBAssist()
dba.insert('ratings', rows, at_once=False)
