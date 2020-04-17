from src.data import clean
from src.data import scrapers
from src.data import transfer
import datetime
import pandas as pd

date = datetime.datetime.now()
start = date.strftime('%Y/%m/%d')

# get team location for upcoming games
end = clean.date_plus(start, 5)
dates = clean.date_range(start, end)

table = scrapers.games.get_scheduled(dates)

transfer.insert('game_scheduled', table, at_once=False, delete=True)
