from data import Scrapers
from data import Transfer
import datetime 

sdate = datetime.date(2019, 11, 15)
edate = datetime.date(2019, 11, 18)

delta = edate - sdate

get_dates = []

for i in range(delta.days + 1):
    date = sdate + datetime.timedelta(days=i)
    date = date.strftime("%Y/%m/%d")
    get_dates.append(date)

#create_query = Transfer.query_from_schema('game_box')
#Transfer.create_from_query(create_query)

for date in get_dates:
    results = Scrapers.game_results(date)
    try:
        Transfer.insert('game_box', results)
    except Exception as e:
        print e
        
