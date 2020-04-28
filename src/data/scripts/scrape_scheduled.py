import datetime
import pandas as pd
from src.data import clean
from src.data import scrapers
from src.data.transfer import DBAssist

date = datetime.datetime.now()
start = date.strftime('%Y/%m/%d')

# get team location for upcoming games
end = clean.date_after_interval(5, start)
dates = clean.dates_in_range(start, end)

table = scrapers.games.get_scheduled(dates)

dba = DBAssist()
dba.replace_rows('game_scheduled')
dba.close()
