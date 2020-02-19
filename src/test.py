from data import Scrapers
from data import Transfer
import datetime 

sdate = datetime.date(2019, 11, 30)
edate = datetime.date(2020, 2, 17)

delta = edate - sdate

get_dates = []

for i in range(delta.days + 1):
    date = sdate + datetime.timedelta(days=i)
    date = date.strftime("%Y/%m/%d")
    get_dates.append(date)

for date in get_dates:
    results = Scrapers.game_results(date)
    try:
        Transfer.insert('game_box', results, at_once=False)
    except Exception as e:
        print e
