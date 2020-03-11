from src.data import Clean
from src.data import scrapers
from src.data import Transfer
import datetime
import pandas as pd

date = datetime.datetime.now()
start = date.strftime('%Y/%m/%d')

# get team location for upcoming games
end = Clean.date_plus(start, 5)
dates = Clean.date_range(start, end)

table = scrapers.games.get_scheduled(dates)

Transfer.insert('game_scheduled', table, at_once=False, delete=True)
