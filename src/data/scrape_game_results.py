import Scrapers, Clean, Transfer
import datetime
import Transfer

date = datetime.datetime.now().date().strftime('%Y/%m/%d')
df = Scrapers.game_results(date)
rows = Clean.dataframe_rows(df)
Transfer.insert('game_scores', rows)
