import os
import pandas as pd
import numpy as np
import sys
sys.path.append("../")
from Cleaning import write_file, combine_files, convert_team_id
from Constants import COLUMNS_TO_RENAME

def change_date(x):
    dl = x.split('/')
    y = dl[-1]
    m = dl[0]
    d = dl[1]
    ds = "_".join([y, m, d])
    return ds
        
def t1_spread(row):
    try:
        spread = float(row['line'])
        team_1 = row['t1_team_id']
        team_road = row['team_id_r']
        if team_1 == team_road:
            spread = 0 - spread
    except:
        spread = np.nan
    return spread
    
def game_id(row):
    dt = row['date_id']
    t1 = str(int(row['t1_team_id']))
    t2 = str(int(row['t2_team_id']))
    gid = "_".join([dt, t1, t2])
    return gid


directory = '../../data/external/pt/'
df = combine_files(directory)

df = df[df['date'].notnull()]
df['date_id'] = df['date'].apply(change_date)

tid = pd.read_csv('../../data/interim/pt_id.csv')
dict_tid = tid.set_index('team').to_dict('index')

df['team_id_h'] = df['home'].apply(lambda x: dict_tid[x]['team_id'])
df['team_id_r'] = df['road'].apply(lambda x: dict_tid[x]['team_id'])

df = df.dropna(how='any', subset=['team_id_h', 'team_id_r'])

df = convert_team_id(df, ['team_id_h', 'team_id_r'], drop=False)

df['spread_t1'] = df.apply(t1_spread, axis=1)

df['game_id'] = df.apply(game_id, axis=1)

df = df[['game_id', 't1_team_id', 't2_team_id', 'spread_t1']]

data_out = '../../data/interim/'

# save school stats data file
write_file(df, data_out, 'spreads', keep_index=True)
