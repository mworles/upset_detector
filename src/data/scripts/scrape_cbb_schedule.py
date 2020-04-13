from src.data import scrapers

for season in range(1993, 2003):
    scrapers.cbb.get_insert_schedules(season)
