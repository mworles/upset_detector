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
        file_names = [x for x in file_names if tag in x]
    
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

def set_gameid_index(df):
    """
    Set dataframe index in DATE_t1##_t2## format as a unique identifier for each
    game. Dataframe must have columns of t1_team_id and t2_team_id.
    DATE: date of game in YYYY_MM_DD format.
    t1##: Lower numerical team ID
    t2##: Higher numerical team ID. 
    """
    id_lower = df['t1_team_id'].astype(str)
    id_upper = df['t2_team_id'].astype(str)
    date = df['date_id']
    df['game_id'] = date + '_' + id_lower + '_' + id_upper
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

def get_score(row, team_id, score_dict):
    row_gameid = row.name
    row_team = row[team_id]
    team_score = score_dict[row_gameid][row_team]
    return team_score

def team_id_scores(df):
    # create dict with key as game identifier, values as team scores
    score_dict = {}
    for i, r in df.iterrows():
        score_dict[i] = {r['wteam']: r['wscore'], r['lteam']: r['lscore']}

    df['t1_score'] = df.apply(lambda x: get_score(x, 't1_team_id', score_dict),
                              axis=1)
    df['t2_score'] = df.apply(lambda x: get_score(x, 't2_team_id', score_dict),
                              axis=1)
    return df
    
def get_t1_win(df, id_cols):
    df = get_scores(df, id_cols)
    df['t1_win'] = np.where(df['t1_score'] > df['t2_score'], 1, 0)
    df = df.drop(columns=['t1_score', 't2_score'])
    return df

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
    df = pd.read_csv('../../data/raw/NCAATourneyCompactResults.csv')
    df.columns = map(lambda x: x.lower(), df.columns)

    # assigns t1_seed to winning teams, t2_seed to losing teams
    df = add_seeds('../../data/raw/', df, ['wteamid', 'lteamid'])

    df = set_gameid_index(df, ['wteamid', 'lteamid'])
    df = df[df.index.isin(idx)]
    
    def label_row(row):
        # absolute seed difference 3 or less, no upset assigned
        if abs(row.t1_seed - row.t2_seed) <= 3:
            return np.nan
        # seed difference 3 or greater, assign 1 for upset
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
