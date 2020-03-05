import os
import pandas as pd
import numpy as np
import Clean
import Odds
import Generate
import re
import Match
import math
import Transfer

def spread_date(x):
    dl = x.split('/')
    y = dl[-1]
    m = dl[0]
    d = dl[1]
    ds = "/".join([y, m, d])
    return ds


def line_format(x, type='spread'):
    try:
        spread = round(float(x), 1)
        if type == 'spread':
            if spread < -50:
                spread = np.nan
            elif spread > 50:
                spread = np.nan
            else:
                pass
        else:
            pass
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

def spreads_pt(datdir):
    df = Clean.combine_files(datdir + '/external/pt/')
    df = df[['date', 'home', 'road', 'line']]

    df = df[df['date'].notnull()]
    df['date'] = df['date'].apply(spread_date)

    # convert spreads to numeric, correct some values
    format_spread = lambda x: line_format(x, type='spread')
    df['spread'] = map(format_spread, df['line'].values)

    df = df.drop('line', axis=1)

    df = Match.id_from_name(df, 'team_pt', 'home', drop=False)
    df = Match.id_from_name(df, 'team_pt', 'road', drop=False)
    
    df = df.dropna(how='any', subset=['home_id', 'road_id'])

    df = Generate.convert_team_id(df, ['home_id', 'road_id'], drop=False)
    
    df['t1_spread'] = np.where(df['t1_team_id'] == df['home_id'], -df['spread'], df['spread'])
    
    df = Generate.set_gameid_index(df, date_col='date', full_date=True,
                                       drop_date=False)
    df = df.sort_index()

    df = df[['date', 't1_team_id', 't2_team_id', 't1_spread']]

    df = df.dropna()
    
    # save school stats data file
    data_out = datdir + '/interim/'
    Clean.write_file(df, data_out, 'spreads_pt', keep_index=True)
    return df

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
        # set cutoff of 60 for calling value a spread
        if int(close_val) < 60:
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
                 'over_under': total,
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
    files = Clean.list_of_files(datdir + 'external/sbro/', tag='ncaa basketball')
    file_data = [parse_sbro(x) for x in files]
    df = pd.concat(file_data, sort=False)
    
    # round spread and total
    format_spread = lambda x: line_format(x, type='spread')
    format_ovun = lambda x: line_format(x, type='line')
    df['spread'] = map(format_spread, df['spread'].values)
    df['over_under'] = map(format_ovun, df['over_under'].values)

    df = Match.id_from_name(df, 'team_sbro', 'away', drop=False)
    df = Match.id_from_name(df, 'team_sbro', 'home', drop=False)

    df['fav_loc'] = np.where(df['favorite'] == df['home'], 'H', 'A')
    df['fav_id'] = np.where(df['fav_loc'] == 'H', df['home_id'], df['away_id'])
    
    df = Generate.convert_team_id(df, ['home_id', 'away_id'], drop=False)

    df['t1_spread'] = np.where(df['t1_team_id'] == df['fav_id'], -df['spread'], df['spread'])
    
    df = Generate.set_gameid_index(df, date_col='date', full_date=True, drop_date=False)
    
    keep_cols = ['date', 't1_team_id', 't2_team_id', 't1_spread', 'over_under']
    df = df[keep_cols]
    df = df.sort_values(['date', 't1_team_id'])
    
    data_out = datdir + '/interim/'
    Clean.write_file(df, data_out, 'spreads_sbro', keep_index=True)
    return df

def blend_spreads(datdir):
    sbro = spreads_sbro(datdir)
    sbro = sbro.rename(columns={'t1_spread': 't1_spread_sbro'})
    
    pt = spreads_pt(datdir)

    pt = pt.rename(columns={'t1_spread': 't1_spread_pt'})
    
    merge_on= ['t1_team_id', 't2_team_id', 'game_id', 'date']
    df = pd.merge(sbro, pt, left_on=merge_on, right_on=merge_on, how='outer')
    
    both_spreads = zip(df['t1_spread_sbro'].values, df['t1_spread_pt'].values)
    
    def pick_spread(spread_tuple):
        if math.isnan(spread_tuple[0]):
            spread = spread_tuple[1]
        else:
            spread = spread_tuple[0]
        return spread

    df['t1_spread'] = map(pick_spread, both_spreads)
    df = df.drop(columns=['t1_spread_sbro', 't1_spread_pt'])
    
    df['season'] = map(Clean.season_from_date, df['date'].values)
    
    df = df.reset_index()
            
    df = df.drop_duplicates(subset='game_id')
    df = df.sort_values('game_id')
    
    df = df.dropna(subset=['t1_spread'])
    
    df['season'] = map(Clean.season_from_date, df['date'].values)
    
    df = df.drop_duplicates()

    return df

def spreads_vi(date=None):
    dba = Transfer.DBAssist()
    dba.connect()
    df = dba.return_df('spreads')
    years = map(lambda x: x.split('-')[0], df['timestamp'].values)
    dates = ["/".join([x, y]) for x, y in zip(years, df['game_date'].values)]
    df['date'] = dates

    
    if date is not None:
        df = df[df['date'] == date]
        # if no data for date, return empty df
        if df.shape[0] == 0:
            return df
        else:
            df = Odds.most_recent_odds(df)
    else:
        most_recent = max(df['timestamp'].values)
        df = df[df['timestamp'] == most_recent]

    df = Match.id_from_name(df, 'team_vi_spreads', 'team_1', drop=False)
    df = Match.id_from_name(df, 'team_vi_spreads', 'team_2', drop=False)
    format_spread = lambda x: line_format(x, type='spread')
    format_ovun = lambda x: line_format(x, type='line')
    df['over_under'] = map(format_ovun, df['line'].values)
    spread_vals = map(format_spread, df['spread'].values)


    fav = np.where(df['team_1'] == df['favorite'], df['team_1_id'], df['team_2_id'])
    df = Generate.convert_team_id(df, ['team_1_id', 'team_2_id'], drop=False)
    t1_fav = df['t1_team_id'] == fav
    vals_t1 = zip(spread_vals, t1_fav)
    df['t1_spread'] = [-x[0] if x[1] == True else x[0] for x in vals_t1]

    df = Generate.set_gameid_index(df, date_col='date', full_date=True,
                                   drop_date=False)

    keep_cols = ['date', 't1_team_id', 't2_team_id', 't1_spread', 'over_under']
    df = df[keep_cols].sort_values('date').reset_index()
    return df

def spreads_by_team(df):
    t1 = df[['game_id', 'date', 't1_team_id', 't1_spread']].copy()
    t1 = t1.rename(columns={'t1_team_id': 'team_id', 't1_spread': 'spread'})
    t2 = df[['game_id', 'date', 't2_team_id', 't1_spread']].copy()
    t2 = t2.rename(columns={'t2_team_id': 'team_id', 't1_spread': 'spread'})
    t2['spread'] = - t2['spread']
    both = pd.concat([t1, t2], sort=False)
    both = both.sort_values('game_id')
    return both
