from src.data import scrapers
from src.data import transfer
import pandas as pd
import time

all_seasons = []

for season in range(2003, 2011):
    teams = scrapers.cbb.srcbb_team_names(season)
    all_seasons.extend(teams)
    time.sleep(1)

df = pd.DataFrame(all_seasons)
df = df.drop_duplicates()

dba = Transfer.DBAssist()
dba.create_insert('team_sched', df, at_once=True)
