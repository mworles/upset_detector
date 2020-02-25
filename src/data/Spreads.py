import os
import pandas as pd
import numpy as np
import Clean
import Odds
import Generate
import re

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
    if row['id_home'] == row['t1_team_id']:
        t1_spread = - t1_spread
    else:
        pass
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

def date_sbro(date, years):
    datelen = len(str(date))
    if datelen == 3:
        month = str(0) + str(date)[:1]
        year = str(years[1])
    else:
        month = str(date)[:2]
        year = str(years[0])
    day = str(date)[-2:]
    date = "/".join([year, month, day])
    return date

def game_sbro(game_rows, col_map):
    teams = [r[col_map['Team']] for r in game_rows]
    decode = lambda x: x.encode('utf-8').strip().decode('ascii', 'ignore')
    teams = [decode(x) for x in teams]
    close = [r[col_map['Close']] for r in game_rows]
    # return value or 0 if value is 'pickem'
    close = [x if x not in ['pk', 'PK'] else 0 for x in close]
    
    nls = [x for x in close if x == 'NL']
    if len(nls) == 2:
        spread, total = '', ''
        fave = teams[1]
    elif len(nls) ==1:
        close_val = [x for x in close if x != 'NL'][0]
        if int(close_val) > 50:
            spread = close_val
            total = 'NL'
            spread_i = close.index(spread)
            fave = teams[spread_i]
        else:
            spread = 'NL'
            total = close_val
            fave = teams[1]
    else:
        spread = min(close)
        spread_i = close.index(spread)
        fave = teams[spread_i]
        
        total = [x for x in close if x != spread][0]

    date = game_rows[0][col_map['Date']]
    game_dict = {'away': teams[0],
                 'home': teams[1],
                 'spread': spread,
                 'total': total,
                 'favorite': fave,
                 'date': date}
    return game_dict


def parse_sbro(file):
    year = int(re.findall(r'[0-9]{4}', file)[0])
    years = [year, year+1]
    
    df = pd.read_excel(file)

    cols = list(df.columns)
    
    col_map = {}
    for c in ['Team', 'Close', 'Date']:
        col_map[c] = cols.index(c)

    vals = df.values

    n_rows = len(df)
    r1 = range(0, n_rows - 1, 2)

    game_rows = [list(vals[x:x+2]) for x in r1]
    
    games = map(lambda x: game_sbro(x, col_map), game_rows)
    
    df = pd.DataFrame(games)
    
    # add year to date
    dates = df['date'].values
    df['date'] = map(lambda x: date_sbro(x, years), dates)

    return df

def spreads_sbro(datdir):
    files = Clean.list_of_files(datdir + 'external/sbro/')
    file_data = [parse_sbro(x) for x in files]
    df = pd.concat(file_data, sort=False)
    return df
    #Clean.write_file(df, datdir + 'external/sbro/', 'spreads_sbro')
