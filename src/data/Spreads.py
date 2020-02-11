import os
import pandas as pd
import numpy as np
import Clean
import Odds
import Generate

def spread_date(x):
    dl = x.split('/')
    y = dl[-1]
    m = dl[0]
    d = dl[1]
    ds = "_".join([y, m, d])
    return ds
        
def spread_format(x):
    try:
        spread = float(x)
        if spread < 0:
            spread = - spread
    except:
        spread = np.nan
    return spread

def spread_favorite(row):
    line = row['line']
    try:
        line = float(line)
        if line > 0:
            fav = row['home']
        elif line < 0:
            fav = row['road']
        else:
            fav = 'NULL'
    except:
        fav = 'NULL'
    return fav

def spread_id(x, id_key):
    try:
        team_id = id_key[x]['team_id']
    except:
        team_id = np.nan
    return team_id
    
def spread_t1(row):
    t1_spread = row['spread']
    if row['home'] == row['favorite']:
        t1_spread = - t1_spread
    return t1_spread

def clean_spreads(datdir):
    datdirectory = '../../data/external/pt/'
    df = Clean.combine_files(datdir + '/external/pt/')
    df = df[['date', 'home', 'road', 'line']]

    df = df[df['date'].notnull()]
    df['game_date'] = df['date'].apply(spread_date)
    df = df.drop('date', axis=1)

    # convert spreads to numeric
    df['spread'] = df['line'].apply(spread_format)

    # identify favorite
    df['favorite'] = df.apply(spread_favorite, axis=1)

    df = df.drop('line', axis=1)

    id_key = Odds.get_id_key(datdir, df, 'team_pt')

    df['id_home'] = df['home'].apply(lambda x: spread_id(x, id_key))
    df['id_road'] = df['road'].apply(lambda x: spread_id(x, id_key))

    df = df.dropna(how='any', subset=['id_home', 'id_road'])

    df['t1_team_id'] = df[['id_home', 'id_road']].apply(min, axis=1).astype(int)
    df['t2_team_id'] = df[['id_home', 'id_road']].apply(max, axis=1).astype(int)

    df['t1_spread'] = df.apply(spread_t1, axis=1)

    df_new = Generate.set_gameid_index(df, date_col='game_date', full_date=True,
                                       drop_date=False)
    df_new = df_new.sort_index()

    df_new = df_new[[ 't1_team_id', 't2_team_id', 't1_spread']]

    data_out = datdir + '/interim/'
    # save school stats data file
    Clean.write_file(df_new, data_out, 'spreads', keep_index=True)
