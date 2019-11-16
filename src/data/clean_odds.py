import json
import re
import pandas as pd
import numpy as np
import calendar
import sys
sys.path.append("../")
from Cleaning import write_file, convert_team_id

def change_date(x):
    dl = x.split('/')
    y = dl[-1]
    m = dl[0]
    d = dl[1]
    ds = "_".join([y, m, d])
    return ds

def encode_line(line):
    line_encoded = [x.encode('utf-8') for x in line]
    return line_encoded

def game_id(row):
    dt = row['date_id']
    t1 = str(int(row['t1_team_id']))
    t2 = str(int(row['t2_team_id']))
    gid = "_".join([dt, t1, t2])
    return gid

dir = "../../data/external/odds/"
df = pd.read_csv(dir + 'odds.csv')

month_key = {v: k for k,v in enumerate(calendar.month_abbr)}

date_o = [x[0] for x in df.values]
date_spl = [x.split('/') for x in date_o]
mon_o = [x[0] for x in date_spl]
mon_n = [str(month_key[x]) for x in mon_o]

def format_month(x):
    if len(x) == 1:
        x = '0' + x
    return x

mon = [format_month(x) for x in mon_n]
year = [x[2] for x in date_spl]
day = [x[1] for x in date_spl]

date_new = ["_".join(x) for x in zip(year, mon, day)]

df['date_id'] = date_new

tid = pd.read_csv('../../data/interim/odds_id.csv')
dict_tid = tid.set_index('team').to_dict('index')

df['team_id_a'] = df['team1'].apply(lambda x: dict_tid[x]['team_id'])
df['team_id_b'] = df['team2'].apply(lambda x: dict_tid[x]['team_id'])

df = df.dropna(how='any', subset=['team_id_a', 'team_id_b'])

df['team_id_a'] = df['team_id_a'].astype(int)
df['team_id_b'] = df['team_id_b'].astype(int)

df = convert_team_id(df, ['team_id_a', 'team_id_b'], drop=False)

def team_odds(row, team):
    team_a = row['team_id_a']
    team_b = row['team_id_b']
    if team=='t1':
        tid = row['t1_team_id']
        if tid == team_a:
            odds = row['odds1']
        else:
            odds = row['odds2']
    elif team == 't2':
        tid = row['t2_team_id']
        if tid == team_b:
            odds = row['odds2']
        else:
            odds = row['odds1']
    else:
        odds = np.nan
    
    odds_numeric = re.sub('[^\d-]+','', odds)
    try:
        odds_numeric = int(odds_numeric)
    except:
        odds_numeric = np.nan

    return odds_numeric


df['t1_odds'] = df.apply(lambda x: team_odds(x, 't1'), axis=1)
df['t2_odds'] = df.apply(lambda x: team_odds(x, 't2'), axis=1)

df['game_id'] = df.apply(game_id, axis=1)

df = df[['t1_team_id', 't2_team_id', 't1_odds', 't2_odds', 'game_id']]

df = df.set_index('game_id')

df = df.sort_index()

write_file(df, '../../data/interim/', 'odds', keep_index=True)
