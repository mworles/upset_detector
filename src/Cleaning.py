import pandas as pd
import os
import re
from fuzzywuzzy import process
from Constants import COLUMNS_TO_RENAME

def write_file(data, data_out, file_name, keep_index=False):
    """Set location and write new .csv file in one line."""
    file = "".join([data_out, file_name, '.csv'])
    data.to_csv(file, index=keep_index)

def combine_files(directory, index_col=False, tag = None):
    """Combine data from all files in a directory."""
    
    # collect names of all files in directory
    file_names = os.listdir(directory)
    
    # if tag given, select file names that include tag
    if tag is not None:
        files_names = [x for x in file_names if tag in x]
    
    # list of full file names for concatenation
    files = [directory + x for x in file_names]
    
    # combine all dataframes
    data_list = [pd.read_csv(x, index_col=index_col) for x in files]
    df = pd.concat(data_list, sort=False)
    
    return df

def clean_school_name(x):
    """Format school name for joining with other data."""
    x = str.lower(x)
    x = re.sub('[().&*\']', '', x)
    x = x.rstrip()
    x = re.sub(r'  ', '-', x)
    x = re.sub(r' ', '-', x)
    return x

def fuzzy_match(x, y, cutoff=90):
    """Indentify the closest match between a given string and a list of
    strings."""
    best_match, score = process.extractOne(x, y)
    if score >= cutoff:
        return best_match
    else:
        return None

def list_files(directory, suffix=".csv"):
    files = os.listdir(directory)
    return [filename for filename in files if filename.endswith(suffix)]

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

def seed_to_numeric(seed):
    new_seed = int(re.sub(r'\D+', '', seed))
    return new_seed

def set_gameid_index(df, id_cols):
    """
    Set dataframe index in YYYY_t1##_t2## format to provide each game with
    a unique identifier.
    YYYY: Season
    t1##: Lower numerical team ID
    t2##: Higher numerical team ID. 
    Provides each game with a unique identifier.
    """
    id_lower = df[id_cols].min(axis=1).astype(str)
    id_upper = df[id_cols].max(axis=1).astype(str)
    season = df['season'].astype(str)
    df['game_id'] = season + '_' + id_lower + '_' + id_upper
    df = df.set_index('game_id')
    return df

def convert_team_id(df, id_cols, drop=True):
    """Create columns with team numerical identifier where t1_team_id is
    numerically lower team, t2_team_id is higher team.
    """
    df['t1_team_id'] = df[id_cols].min(axis=1)
    df['t2_team_id'] = df[id_cols].max(axis=1)
    if drop == True:
        df = df.drop(columns=id_cols)
    return df


def add_seeds(directory, df, team_ids, projected=False):
    # import seeds data file
    seeds = pd.read_csv(directory + 'NCAATourneySeeds.csv')
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

def upset_features(df):
    dfr = df.copy()
    toswitch = dfr['t1_seed'] < dfr['t2_seed']
    t1_cols = [x for x in dfr.columns if x[0:3] == 't1_']
    t2_cols = [x for x in dfr.columns if x[0:3] == 't2_']
    for t1_col, t2_col in zip(t1_cols, t2_cols):
        dfr.loc[toswitch, t1_col] = df.loc[toswitch, t2_col]
        dfr.loc[toswitch, t2_col] = df.loc[toswitch, t1_col]
    return dfr
