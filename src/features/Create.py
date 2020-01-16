import pandas as pd
import os
import data.Clean

def team_seeds(dir):
    data_in = dir + '/scrub/seeds.csv'
    df = pd.read_csv(data_in)
    df['seed'] = df['seed'].apply(data.Clean.get_integer)
    data_out = dir + '/features/'
    data.Clean.write_file(df, data_out, 'team_seeds', keep_index=False)


def merge_features(dir):
    files = data.Clean.combine_files(dir, index_col=False, tag = None)
    print files

"""
# read in team seeds file to get dataset of unique team-seasons
dir = '../../data/'
ts = pd.read_csv(dir + '/raw/NCAATourneySeeds.csv')

# minor cleaning
ts = ts.rename(columns=COLUMNS_TO_RENAME)
ts.columns = ts.columns.str.lower()
ts = ts.drop('seed', axis=1)

# import coach features, merge with team seasons
f_coach = pd.read_csv(dir + 'interim/features_coach.csv')
f = pd.merge(ts, f_coach, how='inner', on=['team_id', 'season'])

# import kp features, merge with other features
f_kp = pd.read_csv(dir + 'interim/features_kp.csv')
f = pd.merge(f, f_kp, how='inner', on=['team_id', 'season'])

# save features data
write_file(f, '../../data/interim/', 'features_all')
"""
