import Scrapers, Clean, Transfer
import datetime
import Transfer

date = datetime.datetime.now()
# will schedule to run overnight, get results for yesterday's date
date = date - datetime.timedelta(days=1)
date = date.strftime('%Y/%m/%d')
rows = Scrapers.game_results(date)
Transfer.insert('game_scores', rows)
