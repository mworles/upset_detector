import Scrapers, Clean, Transfer
import datetime
import Transfer

date = datetime.datetime.now()
# to run overnight, get results for yesterday's date
date = date - datetime.timedelta(days=1)
date = date.strftime('%Y/%m/%d')
df = Scrapers.game_results(date)
rows = Clean.dataframe_rows(df)
Transfer.insert('game_scores', rows)
