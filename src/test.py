import pandas as pd
import Constants
import os
import data
datdir = Constants.DATA
"""
def get_location(row):
    "Returns string indicator of game location for team."
    lteam_dict = {'A': 'H', 'H': 'A', 'N': 'N'}
    if row[0] == row[2]:
        return row[1]
    else:
        return lteam_dict[row[1]]

def team_locations(loc_mat):
    "Given matrix of winner id, winner location, and team id, returns vector of game locations."
    team_loc = map(lambda x: get_location(x), loc_mat)
    return team_loc 

# read in data file with game results
files = data.Clean.list_of_files(datdir + 'scrub/', tag = 'results')
df_list = [pd.read_csv(x) for x in files]

# combine all games to one dataset
df = pd.concat(df_list, sort=False)

# import and merge seasons for dates
s = pd.read_csv(datdir + 'scrub/seasons.csv')
df = pd.merge(df, s, on='season', how='inner')

# add string date column to games
df['date_id'] = df.apply(data.Clean.game_date, axis=1)

# create outcome-neutral team identifier
df = data.Generate.convert_team_id(df, ['wteam', 'lteam'], drop=False)
# create unique game identifier and set as index
df = data.Generate.set_gameid_index(df, full_date=True, drop_date=False)

# add column indicating score for each team
scores = data.Generate.team_scores(df)

# matrix for t1_teams
t1_mat = scores[['wteam', 'wloc', 't1_team_id']].values
t2_mat = scores[['wteam', 'wloc', 't2_team_id']].values

scores['t1_loc'] = team_locations(t1_mat)
scores['t2_loc'] = team_locations(t2_mat)

scores.sort_index()
scores.to_pickle('my_df.pickle')
"""
scores = pd.read_pickle('my_df.pickle')

# adjust score for team location
