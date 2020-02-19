import Scrapers
import Transfer
import datetime 

sdate = datetime.date(2019, 11, 05)
edate = datetime.date(2020, 2, 18)

delta = edate - sdate

get_dates = []

for i in range(delta.days + 1):
    date = sdate + datetime.timedelta(days=i)
    date = date.strftime("%Y/%m/%d")
    get_dates.append(date)

Transfer.create_from_schema('game_scores')
Transfer.create_from_schema('game_box')

for date in get_dates:
    
    game_scores = Scrapers.game_scores(date)
    
    try:
        Transfer.insert('game_scores', game_scores, at_once=False)
    except Exception as e:
        print e

    boxes = Scrapers.get_boxes(date)
    
    for b in boxes:
        try:
            Transfer.insert('game_box', b, at_once=False)
        except Exception as e:
            print e
