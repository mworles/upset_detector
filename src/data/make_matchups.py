import sys
sys.path.append("../")
import pandas as pd
import os
import Clean
from Constants import MIN_YEAR

print 'running %s' % (os.path.basename(__file__))

# read in data file of past game results
dir = '../data/raw/'
df = pd.read_csv(dir + 'NCAATourneyCompactResults.csv')

sk = pd.read_csv(dir + 'Seasons.csv')
sk = sk[['Season', 'DayZero']]

df = pd.merge(df, sk, on='Season', how='inner')

df['date_id'] = df.apply(Clean.game_date, axis=1)

# minor cleaning
df.columns = df.columns.str.lower()

# identify team id columns
team_cols = [x for x in df.columns if 'team' in x]
df = Clean.convert_team_id(df, team_cols)

# remove all columns except season, team id, date_id
keep = ['season', 't1_team_id', 't2_team_id', 'date_id']
df = df[keep]

# add team seeds to data
id_cols = ['t1_team_id', 't2_team_id']
df = Clean.add_seeds(dir, df, id_cols)

# set unique game index
df = Clean.set_gameid_index(df)

# import features data
feat = pd.read_csv('../data/interim/features_all.csv')
#feat = pd.read_csv('../data/interim/features_full.csv')

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

df2 = Clean.set_gameid_index(df2)

"""
#travel distance computation
df1['t1_dist'] = df1.apply(lambda x: compute_travel_distance(x, 't1_team_id'), axis=1)
df2['t2_dist'] = df2.apply(lambda x: compute_travel_distance(x, 't2_team_id'), axis=1)
df2['t1_dist_delt'] = df2['t1_dist'] - df2['t2_dist']
df2['t2_dist_delt'] = df2['t2_dist'] - df2['t1_dist']
"""
# remove some columns not used as features
df2 = df2.drop(['t1_team_id', 't2_team_id', 'date_id'], axis=1)

# keep rows with all features
df2 = df2[df2['season'] >= MIN_YEAR]

Clean.write_file(df2, '../data/processed/', 'features', keep_index=True)
