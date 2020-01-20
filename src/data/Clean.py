import pandas as pd
import numpy as np
import os
import re
import datetime
from fuzzywuzzy import process

def write_file(data, data_out, file_name, keep_index=False):
    """Set location and write new .csv file in one line."""
    file = "".join([data_out, file_name, '.csv'])
    data.to_csv(file, index=keep_index)

def list_of_files(datdirectory, tag = None, tag_drop = None):
    """Create list of all files in a datdirectory."""
    
    # collect names of all files in datdirectory
    file_names = os.listdir(datdirectory)
    
    # if tag given, select file names that include tag
    if tag is not None:
        file_names = [x for x in file_names if tag in x]
    
    # if tag_drop given, remove files with tag
    if tag_drop is not None:
        files_name = [x for x in file_names if tag_drop not in x]
    
    # list of full file names
    files = [datdirectory + x for x in file_names]
    
    return files

def combine_files(datdirectory, index_col=False, tag = None):
    """Combine data from all files in a datdirectory."""
    # list of full file names for concatenation
    files = list_of_files(datdirectory, tag = tag)
    
    # combine all dataframes
    data_list = [pd.read_csv(x, index_col=index_col) for x in files]
    df = pd.concat(data_list, sort=False)
    
    return df

def school_name(x):
    """Format school name for joining with other data."""
    x = str.lower(x)
    x = re.sub('[().&*\']', '', x)
    x = x.rstrip()
    x = re.sub(r'  ', '-', x)
    x = re.sub(r' ', '-', x)
    return x

def fuzzy_match(x, y, cutoff=85):
    """Indentify the closest match between a given string and a list of
    strings."""
    best_match, score = process.extractOne(x, y)
    if score >= cutoff:
        return best_match
    else:
        print 'team not matched'
        print x, best_match, score
        return None

def get_season(file):
    year = re.findall('\d+', file)
    season = int('20' + ''.join(year))
    return season

def add_season(df, season):
    if any([c in df.columns for c in ['Season', 'season']]):
        df = df.rename(columns={'Season': 'season'})
    else:
        df['season'] = season
    return df
    
def game_date(row):
    dn = row['daynum']
    dz = datetime.datetime.strptime(row['dayzero'], '%m/%d/%Y')
    date = dz + datetime.timedelta(days=dn)
    date_id = date.strftime("%Y/%m/%d")
    date_id = date_id.replace('/', '_')
    return date_id


def get_integer(x):
    x_num = int(re.sub(r'\D+', '', x))
    return x_num

def set_gameid_index(df, full_date=False, drop_date=True):
    """
    Set dataframe index in DATE_t1##_t2## format as a unique identifier for each
    game. Dataframe must have columns of t1_team_id and t2_team_id.
    DATE: date of game in YYYY_MM_DD format.
    t1##: Lower numerical team ID
    t2##: Higher numerical team ID. 
    """
    id_lower = df['t1_team_id'].astype(str)
    id_upper = df['t2_team_id'].astype(str)
    
    if full_date == True:
        date = df['date_id']
        if drop_date == True:
            df = df.drop(['date_id'], axis=1)
    else:
        date = df['season'].apply(str)
    
    df['game_id'] = date + '_' + id_lower + '_' + id_upper
    df = df.set_index('game_id')
    
    return df

def convert_team_id(df, id_cols, drop=True):
    """Create team identifier where t1 is numerically lower, t2 is higher."""
    df['t1_team_id'] = df[id_cols].min(axis=1)
    df['t2_team_id'] = df[id_cols].max(axis=1)
    if drop == True:
        df = df.drop(columns=id_cols)
    return df

def team_scores(df):
    
    def get_score(row, team_id, score_dict):
        """Function to apply over rows and obtain score for given team."""
        row_gameid = row.name
        row_team = row[team_id]
        team_score = score_dict[row_gameid][row_team]
        return team_score
    
    # create dict with key as game identifier, values as team scores
    score_dict = {}
    for i, r in df.iterrows():
        score_dict[i] = {r['wteam']: r['wscore'], r['lteam']: r['lscore']}
    
    # apply get_score function to get scores for t1_team and t2_team each row
    df['t1_score'] = df.apply(lambda x: get_score(x, 't1_team_id', score_dict),
                              axis=1)
    df['t2_score'] = df.apply(lambda x: get_score(x, 't2_team_id', score_dict),
                              axis=1)
    return df

def add_seeds(datdirectory, df, team_ids, projected=False):
    # import seeds data file
    seeds = pd.read_csv(datdirectory + 'NCAATourneySeeds.csv')
    # include projected seeds for future matchups
    if projected:
        proj = pd.read_csv('../data/interim/projected_seeds_ids.csv')
        seeds = pd.concat([seeds, proj], sort=False)
    
    # cleaning columns for compatibility
    seeds = seeds.rename(columns=COLUMNS_TO_RENAME)
    seeds.columns = [x.lower() for x in seeds.columns]
    
    # convert seed column to numeric value
    seeds['seed'] = seeds['seed'].apply(seed_to_numeric)
    
    # merge seed values for t1_team
    merge_on1 = ['season', team_ids[0]]
    df_seeds1 = pd.merge(df, seeds, left_on=merge_on1,
                         right_on=['season', 'team_id'], how='inner')
    df_seeds1 = df_seeds1.rename(columns={'seed': 't1_seed'})
    df_seeds1 = df_seeds1.drop(columns=['team_id'])
    
    # merge seed values for t2_team
    merge_on2 = ['season', team_ids[1]]
    df_seeds2 = pd.merge(df_seeds1, seeds, left_on=merge_on2,
                         right_on=['season', 'team_id'], how='inner')
    df_seeds2 = df_seeds2.rename(columns={'seed': 't2_seed'})
    df_seeds2 = df_seeds2.drop(columns=['team_id'])
    
    return df_seeds2

def upset_switch(df):
    """Return boolean array of rows to switch."""
    toswitch = df['t1_seed'] < df['t2_seed']
    return toswitch

def upset_features(df):
    toswitch = upset_switch(df)
    dfr = df.copy()
    t1_cols = [x for x in dfr.columns if x[0:3] == 't1_']
    t2_cols = [x for x in dfr.columns if x[0:3] == 't2_']
    for t1_col, t2_col in zip(t1_cols, t2_cols):
        dfr.loc[toswitch, t1_col] = df.loc[toswitch, t2_col]
        dfr.loc[toswitch, t2_col] = df.loc[toswitch, t1_col]
    return dfr

def team_location(row, team):
    team_col = team + '_team_id'
    tid = row[team_col]
    wloc = row['wloc']
    
    # team is winner
    if row['wteam'] == tid:
        if wloc == 'H':
            # winner is home, and winner=team, so assign 1
            team_loc = 1
        else:
            # winner is away or neutral, and winner=team, assign 0
            team_loc = 0
    
    # team is not winner
    else:
        # winner is away, and winner!=team, so assign 1 
        if wloc == 'A':
            team_loc = 1
        # winner is home or neutral, and winner !=team, so assign 0
        else:
            team_loc = 0
    
    return team_loc

def get_upsets(idx):
    df = pd.read_csv('..//data/raw/NCAATourneyCompactResults.csv')
    df.columns = map(lambda x: x.lower(), df.columns)

    # assigns t1_seed to winning teams, t2_seed to losing teams
    df = add_seeds('../data/raw/', df, ['wteamid', 'lteamid'])
    
    df = convert_team_id(df, ['wteamid', 'lteamid'], drop=False)
    df = set_gameid_index(df, full_date=False)
    df = df[df.index.isin(idx)]

    def label_row(row):
        # absolute seed difference 3 or less, no upset assigned
        if abs(row.t1_seed - row.t2_seed) <= 3:
            return np.nan
        # t1_seed (winner) is 3 or greater, then assign 1 for upset
        elif (row.t1_seed - row.t2_seed) > 3:
            return 1
        # assign 0 for no upset
        else:
            return 0
    
    upset = df.apply(label_row, axis=1)
    return pd.DataFrame({'upset': upset}, index=df.index)

    
def ids_from_index(df):
    """Get team id numbers from the game id index."""
    df.index = df.index.rename('game_id')
    df = df.reset_index()
    df['t1_team_id'] = df['game_id'].apply(lambda x: int(x[5:9]))
    df['t2_team_id'] = df['game_id'].apply(lambda x: int(x[10:]))
    df = df.set_index('game_id')
    return df

def add_team_name(df, datdir='../data/'):
    """Add team names to dataset containing team id numbers."""
    path = "".join([datdir, 'scrub/teams.csv'])
    nm = pd.read_csv(path)
    ido = nm[['team_id', 'team_name']].copy()
    mrg = pd.merge(df, ido, left_on='t1_team_id', right_on='team_id',
                   how='inner')
    mrg = mrg.drop(columns=['team_id'])
    mrg = mrg.rename(columns={'team_name': 'team_1'})
    mrg = pd.merge(mrg, ido, left_on='t2_team_id', right_on='team_id',
                   how='inner')
    mrg = mrg.drop(columns=['team_id'])
    mrg = mrg.rename(columns={'team_name': 'team_2'})
    return mrg

def switch_ids(df, toswitch):
    dfr = df.copy()
    dfr.loc[toswitch, 't1_team_id'] = df.loc[toswitch, 't2_team_id']
    dfr.loc[toswitch, 't2_team_id'] = df.loc[toswitch, 't1_team_id']
    return dfr

def merge_from_list(df_list, merge_on, how='inner'):
    df = df_list[0]
    for x in df_list[1:]:
        df = pd.merge(df, x, on=merge_on, how=how)
    return df
