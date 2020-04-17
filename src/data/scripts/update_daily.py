from src.data import updater
import datetime

date = datetime.datetime.now()
# will schedule to run overnight, get results for yesterday's date
date = date - datetime.timedelta(days=1)
date = date.strftime('%Y/%m/%d')

updater.update_day(date)
