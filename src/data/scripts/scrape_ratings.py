from src.data import scrapers
from src.data.transfer import DBAssist

rows = scrapers.ratings.run()
dba = DBAssist()
dba.insert_rows('ratings', rows)
dba.close()
