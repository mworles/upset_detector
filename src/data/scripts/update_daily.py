from src.data import Updater
import datetime

date = datetime.datetime.now()
# will schedule to run overnight, get results for yesterday's date
date = date - datetime.timedelta(days=1)
date = date.strftime('%Y/%m/%d')

date = '2020/02/28'
Updater.update_day(date)
