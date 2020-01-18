import pandas as pd
import numpy as np
import os
import sys
sys.path.append('../')
from Constants import COLUMNS_TO_RENAME
from Cleaning import combine_files, convert_team_id, set_gameid_index
from Cleaning import team_id_scores, team_location, write_file
from Clean import game_date
import datetime

# read in data file of past game results
datdir = '../../data/raw/'

df = combine_files(datdir, tag='CompactResults')

sk = pd.read_csv(datdir + 'Seasons.csv')
sk = sk[['Season', 'DayZero']]

df = pd.merge(df, sk, on='Season', how='inner')

df['date_id'] = df.apply(game_date, axis=1)

df = df.rename(columns=COLUMNS_TO_RENAME)
df.columns = df.columns.str.lower()

# set index as unique game identifier
df = convert_team_id(df, ['wteam', 'lteam'], drop=False)
df = set_gameid_index(df)
df = team_id_scores(df)

df['t1_win'] = np.where(df['t1_score'] > df['t2_score'], 1, 0)

df['t1_marg'] = df['t1_score'] - df['t2_score']

df['t1_home'] = df.apply(lambda x: team_location(x, 't1'), axis=1)
df['t2_home'] = df.apply(lambda x: team_location(x, 't2'), axis=1)

keep_cols = ['t1_team_id', 't2_team_id', 't1_score', 't2_score', 't1_win',
             't1_marg', 't1_home', 't2_home']
df = df[keep_cols]

data_out = '../../data/interim/'

write_file(df, data_out, 'games', keep_index=True)
