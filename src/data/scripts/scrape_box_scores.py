from src.data import scrapers
from src.data import Transfer
import datetime

"""
date = datetime.datetime.now()
# will schedule to run overnight, get results for yesterday's date
date = date - datetime.timedelta(days=1)
date = date.strftime('%Y/%m/%d')
"""
date = '2020/03/06'

game_scores = scrapers.games.game_scores(date)

try:
    Transfer.insert('game_scores', game_scores, at_once=False)
except Exception as e:
    print e

boxes = scrapers.games.box_scores(date)

for b in boxes:
    try:
        Transfer.insert('game_box', b, at_once=False)
    except Exception as e:
        print e
