import Updater

dates = Clean.date_range("2020/02/25", end_date="2020/02/29")

for date in dates:
    Updater.update_day_tmp(date)
