import json
import pandas as pd
import numpy as np
import calendar
import Clean
import Generate

def odds_games(year_file):
    
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

def parse_odds(datdir):
    all_games = []

    year_files = Clean.list_of_files(datdir)
    all_games = [odds_games(file) for file in year_files]
    all_flat = [game for year in all_games for game in year]

    encode_cell = lambda x: x.encode('ascii', 'ignore')

    all_flat = [[encode_cell(x) for x in game] for game in all_flat]

    return all_flat

def odds_dates(date_values):
        
    month_key = {v: k for k,v in enumerate(calendar.month_abbr)}

    date_spl = [x.split('/') for x in date_values]
    
    tp = [x for x in date_spl if x[0] == '08']
    mon_o = [x[0] for x in date_spl]
    mon_n = [str(month_key[x]) for x in mon_o]
    mon = ['0' + x if len(x) == 1 else x for x in mon_n]
    year = [x[2] for x in date_spl]
    day = [x[1] for x in date_spl]

    date_new = ["_".join(x) for x in zip(year, mon, day)]
    
    return date_new

def format_odds(x):
    x = x.replace('+', '')
    try:
        result = float(x)
    except:
        result = np.nan
    
    return result

def get_id_key(datdir, df, name_col):
    tid = pd.read_csv(datdir + '/interim/id_key.csv')
    # remove if missing or duplicated
    tid = tid[['team_id', name_col]]
    tid = tid.rename(columns={name_col: 'name'})
    tid = tid[~tid['name'].duplicated()]
    dict_tid = tid.set_index('name').to_dict('index')
    return dict_tid

def order_odds(row, id_key):    
    try:
        t1 = id_key[row['team_1']]['team_id']
    except:
        t1 = np.nan
    try:
        t2 = id_key[row['team_2']]['team_id']
    except:
        t2 = np.nan
    
    # lower numeric id first or second
    lower_i = [t1, t2].index(min([t1, t2]))
    
    if lower_i == 0:
        return [row['game_date'], t1, t2, row['odds1'], row['odds2']]

    else:
        return [row['game_date'], t2, t1, row['odds2'], row['odds1']]


def clean_odds(datdir):
    oddsdir = datdir + "/external/odds/"
    data = parse_odds(oddsdir)
    col_names = ['date', 'team_1', 'team_2', 'odds1', 'odds2']
    df = pd.DataFrame(data, columns=col_names)

    date_new = odds_dates(df['date'].values)
    df['game_date'] = date_new
    df = df.drop('date', axis=1)
    df = df.sort_values('game_date', ascending=True)


    odds1 = df['odds1'].apply(format_odds).values
    odds2 = df['odds2'].apply(format_odds).values
    df = df.drop(['odds1', 'odds2'], axis=1)
    df['odds1'] = odds1
    df['odds2'] = odds2

    df = df[['game_date', 'team_1', 'team_2', 'odds1', 'odds2']]

    id_key = get_id_key(datdir, df, 'team_oddsport')
    result = df.apply(lambda x: order_odds(x, id_key), axis=1).tolist()
    cols_new = ['game_date', 't1_team_id', 't2_team_id', 't1_odds', 't2_odds']
    df_new = pd.DataFrame(result, columns=cols_new)
    
    df_new = df_new.dropna(subset=['t1_team_id', 't2_team_id'])  
    
    df_new['t1_team_id'] = df_new['t1_team_id'].astype(int)
    df_new['t2_team_id'] = df_new['t2_team_id'].astype(int)
    
    df_new = Generate.set_gameid_index(df_new, date_col='game_date',
                                       full_date=True, drop_date=False)
    

    Clean.write_file(df_new, datdir + '/interim/', 'odds', keep_index=True)
