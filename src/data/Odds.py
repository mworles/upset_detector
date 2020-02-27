import json
import pandas as pd
import numpy as np
import calendar
import Clean, Generate, Match, Transfer

def oddsportal_games(year_file):
    
    year_games = []

    with open(year_file, 'r') as f:
        year_data = json.load(f)

    pages = year_data.keys()

    for p in pages:
        page_data = year_data[p]
        # correct problematic dates for games today and yesterday
        page_data = [g for g in page_data if 'Today' not in g[0]]
        page_data = [g for g in page_data if 'Yesterday' not in g[0]]
        year_games.extend(page_data)

    return year_games

def parse_oddsportal(datdir):
    all_games = []

    year_files = Clean.list_of_files(datdir)
    all_games = [oddsportal_games(file) for file in year_files]
    all_flat = [game for year in all_games for game in year]

    encode_cell = lambda x: x.encode('ascii', 'ignore')

    all_flat = [[encode_cell(x) for x in game] for game in all_flat]

    return all_flat

def oddsportal_dates(date_values):
        
    month_key = {v: k for k,v in enumerate(calendar.month_abbr)}

    date_spl = [x.split('/') for x in date_values]
    
    tp = [x for x in date_spl if x[0] == '08']
    mon_o = [x[0] for x in date_spl]
    mon_n = [str(month_key[x]) for x in mon_o]
    mon = ['0' + x if len(x) == 1 else x for x in mon_n]
    year = [x[2] for x in date_spl]
    day = [x[1] for x in date_spl]

    date_new = ["/".join(x) for x in zip(year, mon, day)]
    
    return date_new

def format_odds(x):
    x = x.replace('+', '')
    try:
        result = float(x)
    except:
        result = np.nan
    
    return result

def most_recent_odds(df):
    gbc = ['game_date', 'team_1', 'team_2']
    dfgb = df.groupby(gbc)
    dfgb = dfgb.apply(lambda x: x.sort_values(['timestamp'], ascending=False))
    dfgb = dfgb.reset_index(drop=True).groupby(gbc).head(1)
    return dfgb

def get_odds_dicts(df):
    grid = df[['odds1', 'odds2', 'team_1_id', 'team_2_id']].values
    game_dict = lambda x: {x[2]: x[0], x[3]: x[1]}
    l_games = map(game_dict, grid)
    return l_games

def team_odds(df, game_dicts):
    t1_id = df['t1_team_id'].values
    t2_id = df['t2_team_id'].values
    df['t1_odds'] = [dict[team] for dict, team in zip(game_dicts, t1_id)]
    df['t2_odds'] = [dict[team] for dict, team in zip(game_dicts, t2_id)]
    return df



def clean_oddsportal(datdir):
    oddsdir = datdir + "/external/odds/"
    data = parse_oddsportal(oddsdir)
    col_names = ['date', 'team_1', 'team_2', 'odds1', 'odds2']
    df = pd.DataFrame(data, columns=col_names)

    date_new = oddsportal_dates(df['date'].values)
    df = df.drop('date', axis=1)
    df['date'] = date_new

    odds1 = df['odds1'].apply(format_odds).values
    odds2 = df['odds2'].apply(format_odds).values
    df = df.drop(['odds1', 'odds2'], axis=1)
    df['odds1'] = odds1
    df['odds2'] = odds2

    df = Match.id_from_name(df, 'team_oddsport', 'team_1', drop=False)
    df = Match.id_from_name(df, 'team_oddsport', 'team_2', drop=False)
    
    l_games = get_odds_dicts(df)
    df = Generate.convert_team_id(df, ['team_1_id', 'team_2_id'], drop=False)
    df = team_odds(df, l_games)

    df = df.dropna(subset=['t1_team_id', 't2_team_id'])  
    
    df['t1_team_id'] = df['t1_team_id'].astype(int)
    df['t2_team_id'] = df['t2_team_id'].astype(int)
    
    df = Generate.set_gameid_index(df, date_col='date', full_date=True, drop_date=False)
    
    keep_cols = ['date', 't1_team_id', 't2_team_id', 't1_odds', 't2_odds']
    df = df[keep_cols].sort_values('date').reset_index()
    
    # drop rows with no odds
    df = df.dropna(subset=['t1_odds', 't2_odds'], how='all')
    
    return df

def odds_vi(date=None):
    dba = Transfer.DBAssist()
    dba.connect()
    df = dba.return_df('odds')

    years = map(lambda x: x.split('-')[0], df['timestamp'].values)
    dates = ["/".join([x, y]) for x, y in zip(years, df['game_date'].values)]
    df['date'] = dates
    
    if date is not None:
        df = df[df['date'] == date]
        df = most_recent_odds(df)
    else:
        most_recent = max(df['timestamp'].values)
        df = df[df['timestamp'] == most_recent]
    
    df = Match.id_from_name(df, 'team_vi_odds', 'team_1', drop=False)
    df = Match.id_from_name(df, 'team_vi_odds', 'team_2', drop=False)

    l_games = get_odds_dicts(df)
    df = Generate.convert_team_id(df, ['team_1_id', 'team_2_id'], drop=False)
    df = team_odds(df, l_games)

    df = Generate.set_gameid_index(df, date_col='date', full_date=True, drop_date=False)
    keep_cols = ['date', 't1_team_id', 't2_team_id', 't1_odds', 't2_odds']
    df = df[keep_cols].sort_values('date').reset_index()
    
    return df
