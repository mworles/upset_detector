from src.data import scrapers
from src.data.transfer import DBAssist
import datetime

dba = DBAssist()
dba.create_from_schema('game_scores')

date = datetime.datetime.now()
# will schedule to run overnight, get results for yesterday's date
date = date - datetime.timedelta(days=1)
date = date.strftime('%Y/%m/%d')

game_scores = scrapers.games.game_scores(date)

try:
    dba.insert_rows('game_scores', game_scores, at_once=False)
except Exception as e:
    print e

boxes = scrapers.games.box_scores(date)

for b in boxes:
    try:
        dba.insert_rows('game_box', b, at_once=False)
    except Exception as e:
        print e

dba.close()
