import json
import pandas as pd
import numpy as np
import calendar
import Clean
import Generate
import Match
import Transfer
import datetime

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
    
    # remove duplicates
    df = df.drop_duplicates(subset=['date', 'team_1', 'team_2'])

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
    
    df['season'] = map(Clean.season_from_date, df['date'].values)
    
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
        # if no data for date, return empty df
        if df.shape[0] == 0:
            return df
        else:
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
    
def decimal_odds(x):
    if x < 0:
        decimal = (100/abs(x)) + 1
    else:
        decimal =  (x/100) +1
    return round(decimal, 3)

def to_datetime(x):
    dt = datetime.datetime.strptime(x, "%Y/%m/%d")
    return dt

def from_datetime(x):
    date = x.strftime("%Y/%m/%d")
    return date
    
def proximal_date(date_gid, odds_dict):
    # get datetime from game string date
    game_date = to_datetime(date_gid[0])
    after = from_datetime(game_date + datetime.timedelta(days=1))
    before = from_datetime(game_date - datetime.timedelta(days=1))
    # string dates before and after game string date
    valid = [before, after]
    
    # dates in odds matching the game's year-teams 
    odds_dates = odds_dict[date_gid[1]]
    
    # list of common dates
    overlap = list(set(odds_dates) & set(valid))
    
    if len(overlap) == 0:
        result = ''
        
    elif len(overlap) == 1:
        result = overlap[0]
    
    else:
        result = ''
  
    return result


def matchup_odds_dates(mat, odds):
    # create temp game id using season instead of date year
    # to find similar matchups using only year and teams, not specific date 
    mat['team_string'] = mat['game_id'].apply(lambda x: x[-10:])
    mat['gid_sub'] = mat['season'].astype(int).astype(str) + mat['team_string']
    
    # get first matchup date for each season
    min_date = mat.groupby('season')['date'].min().reset_index()
    min_date = min_date.rename(columns={'date': 'min_date'})

    # merge minimum matchups date and keep odds dates after it, each season
    odds = pd.merge(odds, min_date, how='inner', left_on='season',
                    right_on='season')
    odds = odds[odds['date'] >= odds['min_date']]
        
    # create temp game id similar to match 
    odds['gid_sub'] = odds['game_id'].apply(lambda x: x[-10:])
    odds['gid_sub'] = odds['season'].astype(int).astype(str) + odds['gid_sub']
    
    # dict with key for each unique gid_sub, val is empty list
    odds_dict = {k : [] for k in pd.unique(odds['gid_sub'].values) }
    
    # array of gid_sub, date pairs
    odds_pairs = odds[['gid_sub', 'date']].values
    
    # update each gid value list with date
    # so dict contains all dates for each unique gid_sub
    for row in odds_pairs:
        odds_dict[row[0]].append(row[1])
    
    # restrict games to match where game has 'gid_sub' in the dict to search
    mat_find = mat[mat['gid_sub'].isin(odds_dict.keys())].copy()
    drop_index = mat_find.index
    mat_not = mat.drop(drop_index).copy()
    
    # array of year-teams string and dates to search
    mat_pairs = zip(mat_find['date'].values, mat_find['gid_sub'].values)
    odds_dates = map(lambda x: proximal_date(x, odds_dict), mat_pairs)
    
    # creating matching game id using proximal date
    proxdate = map(lambda x: x.replace('/', '_'), odds_dates)
    mat_find['odds_gid'] = proxdate + mat_find['team_string']
    
    # select odds columns needed to merge
    odds_merge = odds[['game_id', 't1_odds', 't2_odds']].copy()
    odds_merge = odds_merge.rename(columns={'game_id': 'odds_gid'})
    
    mrg = pd.merge(mat_find, odds_merge, how='left', left_on='odds_gid', 
                   right_on='odds_gid')
        
    # remove temp columns used for matching
    mrg = mrg.drop(columns=['team_string', 'gid_sub', 'odds_gid'])
    
    
    # combined games matched and games not
    df = pd.concat([mrg, mat_not], sort=False)
    
    df = df.sort_values('game_id')
    
    return df


def odds_table():
    mat = Transfer.return_data('game_info')
    
    odds = Odds.clean_oddsportal(datdir)

    # only have odds starting in season 2009
    mat_get = mat[mat['season'] >= 2009].copy()

    # first try inner merge
    odds_merge = odds[['game_id', 't1_odds', 't2_odds']].copy()
    mrg = pd.merge(mat_get, odds_merge, how='inner', left_on='game_id',
                   right_on='game_id')

    # remove merged games from odds and games
    merged_gid = mrg['game_id'].values
    odds_prox = odds[~odds['game_id'].isin(merged_gid)].copy()
    mat_get = mat_get[~mat_get['game_id'].isin(merged_gid)].copy()

    # use function to match on proximal dates
    mat_get = Odds.matchup_odds_dates(mat_get, odds_prox)

    # combine merged, proximal date-matched games, and pre-odds games
    df = pd.concat([mrg, mat_get, mat[mat['season'] < 2009]], sort=False)
    
    # keep data unique to odds or for merging with other tables
    df = df[['game_id', 'date', 'season', 't1_team_id', 't2_team_id', 't1_odds',
             't2_odds']]
    
    # compute decimal odds format
    df['t1_odds_dec'] = map(Odds.decimal_odds, df['t1_odds'].values)
    df['t2_odds_dec'] = map(Odds.decimal_odds, df['t2_odds'].values)

    # only keep rows with odds data
    df = df.dropna(subset=['t1_odds', 't2_odds'], how='all')

    return df
