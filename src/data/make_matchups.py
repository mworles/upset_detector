import sys
sys.path.append("../")
import pandas as pd
import os
from Cleaning import write_file, set_gameid_index, convert_team_id, add_seeds
from Constants import CURRENT_YEAR

print 'running %s' % (os.path.basename(__file__))

# read in data file of past game results
dir = '../../data/raw/'
df = pd.read_csv(dir + 'NCAATourneyCompactResults.csv')

# minor cleaning
df.columns = df.columns.str.lower()

# identify team id columns
team_cols = [x for x in df.columns if 'team' in x]
df = convert_team_id(df, team_cols)

# remove all columns except season and team id
keep = ['season', 't1_team_id', 't2_team_id']
df = df[keep]

# add team seeds to data
id_cols = keep[-2:]
df = add_seeds(dir, df, id_cols)

# set unique game index
df = set_gameid_index(df, id_cols)

# import features data
feat = pd.read_csv('../../data/interim/features_all.csv')
# remove current year, separate script used for future matchups
feat = feat[feat['season'] != CURRENT_YEAR]

# create dataframe of features for t1 teams
exc = ['season', 'team_id']
tcols = [x for x in list(feat.columns) if x not in exc]

t1 = feat.copy()
t1cols = exc + ['t1_' + x for x in tcols]
t1.columns = t1cols

t2 = feat.copy()
t2cols = exc + ['t2_' + x for x in tcols]
t2.columns = t2cols

df1 = pd.merge(df, t1, left_on=['season', 't1_team_id'], right_on=exc,
               how='inner')
df1 = df1.drop('team_id', axis=1)

df2 = pd.merge(df1, t2, left_on=['season', 't2_team_id'], right_on=exc,
               how='inner')
df2 = df2.drop('team_id', axis=1)

df2 = set_gameid_index(df2, id_cols)

"""
travel distance computation
df1['t1_dist'] = df1.apply(lambda x: compute_travel_distance(x, 't1_team_id'), axis=1)
df2['t2_dist'] = df2.apply(lambda x: compute_travel_distance(x, 't2_team_id'), axis=1)
df2['t1_dist_delt'] = df2['t1_dist'] - df2['t2_dist']
df2['t2_dist_delt'] = df2['t2_dist'] - df2['t1_dist']
"""
write_file(df2, '../../data/processed/', 'features', keep_index=True)
