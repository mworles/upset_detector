import pandas as pd
import Constants
import os
import data
datdir = Constants.DATA
"""
# read in data file with game results
files = data.Clean.list_of_files(datdir + 'scrub/', tag = 'results')
df_list = [pd.read_csv(x) for x in files]

df = pd.concat(df_list, sort=False)

s = pd.read_csv(datdir + 'scrub/seasons.csv')
df = pd.merge(df, s, on='season', how='inner')

# add string date column to games
df['date_id'] = df.apply(data.Clean.game_date, axis=1)

# created outcome-neutral team identifier
df = data.Generate.convert_team_id(df, ['wteam', 'lteam'], drop=False)
# create unique game identifier and set as index
df = data.Generate.set_gameid_index(df, full_date=True, drop_date=False)

# add column indicating score for each team
scores = data.Generate.team_scores(df)

scores.to_pickle('my_df.pickle')
"""
scores = pd.read_pickle('my_df.pickle')
scores = scores.sort_index()

def get_location(row):
    lteam_dict = {'A': 'H', 'H': 'A', 'N': 'N'}
    if row[0] == row[2]:
        return row[1]
    else:
        return lteam_dict[row[1]]

scores = scores.iloc[0:20, :]
print scores.head()

t1_loc = map(lambda x: get_location(x), scores[['wteam', 'wloc', 't1_team_id']].values)

print t1_loc
