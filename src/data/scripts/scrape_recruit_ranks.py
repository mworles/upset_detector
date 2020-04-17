from src.data import scrapers
from src.data import transfer

seasons = range(2003, 2020)

for season in seasons:
    browser = scrapers.utils.set_browser('../scrapers/driver/chromedriver.exe')
    players = scrapers.recruit.players_from_season(season, browser)
    transfer.insert('recruit_ranks', players, at_once=False)
