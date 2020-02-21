import Scrapers
import Transfer
import datetime 

date = datetime.datetime.now()
# will schedule to run overnight, get results for yesterday's date
date = date - datetime.timedelta(days=1)
date = date.strftime('%Y/%m/%d')

#Transfer.create_from_schema('game_scores', 'schema.json')
#Transfer.create_from_schema('game_box', 'schema.json')
date = ('2020/02/19')    
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
