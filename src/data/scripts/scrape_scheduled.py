from src.data import Clean
from src.data import scrapers
from src.data import Transfer
import datetime
import pandas as pd

date = datetime.datetime.now()
start = date.strftime('%Y/%m/%d')
start = '2020/02/29'

# get team location for upcoming games
end = Clean.date_plus(start, 5)
dates = Clean.date_range(start, end)

scheduled = []

for date in dates:
    results = scrapers.games.game_scores(date, future=True)    
    if len(scheduled) == 0:
        scheduled.extend(results)
    else:
        scheduled.extend(results[1:])

table = scrapers.games.get_scheduled(dates)

Transfer.insert('game_scheduled', table, at_once=False, delete=True)
