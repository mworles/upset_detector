import pandas as pd
import numpy as np
import data.Clean

def team_seeds(datdir):
    data_in = datdir + '/scrub/seeds.csv'
    df = pd.read_csv(data_in)
    df['seed'] = df['seed'].apply(data.Clean.get_integer)
    data_out = datdir + '/features/'
    data.Clean.write_file(df, data_out, 'team_seeds', keep_index=False)

def team_ratings(datdir):
    datdirectory = datdir + '/external/kp/'

    # create list of file names
    files = data.Clean.list_of_files(datdirectory)

    # use files to get lists of season numbers and dataframes
    seasons = [data.Clean.get_season(x) for x in files]
    dfs = [pd.read_csv(x) for x in files]

    # add season column
    data_list = [data.Clean.add_season(x, y) for x, y  in zip(dfs, seasons)]

    df = pd.concat(data_list, sort=False)
    
    # link team id numbers
    id_file = datdir + '/interim/id_key.csv'
    id = pd.read_csv(id_file)
    id = id[['team_id', 'team_kp']]
    mrg = pd.merge(df, id, left_on='TeamName', right_on='team_kp', how='inner')
    mrg = mrg.drop(['team_kp'], axis=1)
    
    # for consistency
    mrg.columns = map(str.lower, mrg.columns)
    
    # fill missing rows due to changes in column name
    mrg['em'] = np.where(mrg['em'].isnull(), mrg['adjem'], mrg['em'])
    mrg['rankem'] = np.where(mrg['rankem'].isnull(), mrg['rankadjem'], mrg['rankem'])

    # select columns to keep as features
    keep = ['team_id', 'season', 'adjtempo', 'adjoe', 'rankadjoe', 'adjde', 
            'rankadjde', 'em', 'rankem']
    mrg = mrg[keep]

    # save team ratings file
    data_out = datdir + 'features/'
    data.Clean.write_file(mrg, data_out, 'team_ratings')

def coach_features(datdir):
    shift = lambda x: x.shift(1)
        
    def create_shift(df, col, new_col, func):
        df[new_col] = df.groupby('coach_name')[col].apply(func)
        df[new_col] = df.groupby('coach_name')[new_col].apply(shift)
        df[new_col] = df[new_col].fillna(0)
        return df
    
    # read in data files
    to = pd.read_csv(datdir + 'interim/tourney_outcomes.csv')
    c = pd.read_csv(datdir + 'scrub/coaches.csv')

    # merge coach file with team tourney outcomes file
    df = pd.merge(c, to, how='outer', on=['season', 'team_id'])

    # add last day for each team to account for removed coaches
    df['team_last'] = df.groupby(['team_id', 'season'])['last_day'].transform(max)
    
    # if coach wasn't still with team for tourney, recode wins and games
    df['wins'] = np.where(df.last_day != df.team_last, np.nan, df['wins'])
    df['games'] = np.where(df.last_day != df.team_last, np.nan, df['games'])
    
    # if missing the true value is 0
    df['games'] = df['games'].fillna(0)
    df['wins'] = df['wins'].fillna(0)

    # first sort before creating shifted variables
    df = df.sort_values(['coach_name', 'season'])
    
    # 0/1 indicator of whether coach coached in tourney, for creating features
    df['cvis'] = np.where(df['games'] > 0, 1, 0)

    # functions for applying on grouped data
    cumsum = lambda x: x.cumsum()
    cummax = lambda x: x.cummax()
    
    # indicator of whether coach was in prior year's tourney
    df['clast'] = df.groupby('coach_name')['cvis'].apply(shift)
    # if missing, row is coach's first year and value should be 0
    df['clast'] = np.where(df['clast'].isnull(), 0, df['clast'])

    # count of coach's prior tourneys
    df = create_shift(df, 'cvis', 'cvisits', cumsum)
    # max round coach has previously reached
    df = create_shift(df, 'games', 'cfar', cummax)
    # indicator if coach has won tourney game or not
    df['cwon'] = np.where(df['cfar'] > 1, 1, 0)
    
    # 0/1 indicator for row if made elite 8 (round 4)
    d_round = dict.fromkeys([0, 1, 2, 3], 0)
    d_round.update(d_round.fromkeys([4, 5, 6], 1))
    df['round_4'] = df['games'].map(d_round)
    
    # 0/1 indicator for row if made final 4 (round 5)
    d_round[4] = 0
    df['round_5'] = df['games'].map(d_round)
    
    # number of elite 8 trips
    df = create_shift(df, 'round_4', 'c_r4sum', cumsum)
    # number of final 4 trips
    df = create_shift(df, 'round_5', 'c_r5sum', cumsum)
    
    # indicator of many trips without deep tourney success
    df['snkbit'] = np.where((df['cvisits'] > 5) & (df['cfar'] < 4), 1, 0)
    
    # one row per team season, 
    df = df[df.last_day == df.team_last]
    
    # keep only coach features and unique keys
    cols_drop = ['cvis', 'wins', 'games', 'round_4', 'round_5', 'first_day',
                 'last_day','team_last', 'coach_name']
    df = df.drop(cols_drop, axis=1)
    
    # no tourney data prior to season 1985, remove rows
    df = df[df['season'] > 1985]
    
    data.Clean.write_file(df, datdir + 'features/', 'coach')

def filter_teams(datdir, df):
    data_in = datdir + '/scrub/seeds.csv'
    left = pd.read_csv(data_in)
    merge_on = ['team_id', 'season']
    left = left[merge_on]
    df = pd.merge(left, df, on=merge_on, how='left')
    return df

def merge_features(datdir):
    subdatdir = datdir + '/features/'
    files = data.Clean.list_of_files(subdatdir, tag = None)
    read_csv = lambda x: pd.read_csv(x)
    df_list = [read_csv(x) for x in files]
    merge_on = ['team_id', 'season']
    df = data.Clean.merge_from_list(df_list, merge_on, how='outer')
    # filter to tourney teams
    df = filter_teams(datdir, df)
    data_out = datdir + '/features/'
    data.Clean.write_file(df, data_out, 'team_features', keep_index=False)
