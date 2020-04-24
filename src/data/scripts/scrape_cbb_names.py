from src.data import scrapers
from src.data.transfer import DBAssist
import pandas as pd
import time

all_seasons = []


for season in range(2003, 2011):
    teams = scrapers.cbb.srcbb_team_names(season)
    all_seasons.extend(teams)
    time.sleep(1)

df = pd.DataFrame(all_seasons)
df = df.drop_duplicates()

dba.DBAssist()
dba.create_from_data('team_sched', df)
dba.insert_rows('team_sched', df)
dba.close()
