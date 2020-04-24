from src.data import scrapers
from src.data.transfer import DBAssist

seasons = range(2003, 2020)
dba = DBAssist()

for season in seasons:
    browser = scrapers.utils.set_browser('../scrapers/driver/chromedriver.exe')
    players = scrapers.recruit.players_from_season(season, browser)
    dba.insert_rows('recruit_ranks', players, at_once=True)

dba.close()
