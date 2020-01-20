"""Create features

This module contains functions used to create features. Features are calculated
from raw data or 'interim' data created by the data pipeline. Features are 
specific to each unique team-season, for example the Kentucky 2012 team and 
Kentucky 2013 team have different features. 

This script requires `pandas` and `numpy`. It imports the custom Clean module.

"""
import pandas as pd
import numpy as np
import data.Clean

def team_seeds(datdir):
    """Create data containing team seeds."""
    # read preprocessed seed data
    data_in = datdir + '/scrub/seeds.csv'
    df = pd.read_csv(data_in)
    
    # obtain the integer value from string seed
    df['seed'] = df['seed'].apply(data.Clean.get_integer)
    
    # save data file
    data_out = datdir + '/features/'
    data.Clean.write_file(df, data_out, 'team_seeds', keep_index=False)

def team_ratings(datdir):
    """Create data containing team ratings."""
    
    ratings_dir = datdir + '/external/kp/'
    
    # create list of file names from directory
    files = data.Clean.list_of_files(ratings_dir)

    # use files to get lists of season numbers and dataframes
    # data has no season column so must be collected from file name and added
    seasons = [data.Clean.get_season(x) for x in files]
    dfs = [pd.read_csv(x) for x in files]

    # add season column
    data_list = [data.Clean.add_season(x, y) for x, y  in zip(dfs, seasons)]

    # create combined data with all seasons
    df = pd.concat(data_list, sort=False)

    # ratings data has team names, must be linked to numeric ids
    df = data.Match.id_from_name(datdir, df, 'team_kp', 'TeamName')
    
    # for consistency
    df.columns = map(str.lower, df.columns)

    # fill missing rows due to changes in column name
    df['em'] = np.where(df['em'].isnull(), df['adjem'], df['em'])
    df['rankem'] = np.where(df['rankem'].isnull(), df['rankadjem'], df['rankem'])

    # reduce float value precision
    df = data.Clean.round_floats(df, prec=2)

    # select columns to keep as features
    keep = ['team_id', 'season', 'adjtempo', 'adjoe', 'rankadjoe', 'adjde', 
            'rankadjde', 'em', 'rankem']
    df = df[keep]

    # save team ratings file
    data_out = datdir + 'features/'
    data.Clean.write_file(df, data_out, 'team_ratings')

def coach_features(datdir):
    """Create data containing coach features."""
    
    # create functions to use locally with grouped data
    # shift row value down
    shift = lambda x: x.shift(1)
    # computes cumulative sum
    cumsum = lambda x: x.cumsum()
    # computes cumulative max
    cummax = lambda x: x.cummax()
        
    def create_shift(df, col, new_col, func):
        """General function used to create new column and shift value."""
        # group by coach and apply given function on given column
        df[new_col] = df.groupby('coach_name')[col].apply(func)
        # shift values down by one row, indicators reflect past performance
        df[new_col] = df.groupby('coach_name')[new_col].apply(shift)
        # any null values should be treated as 0
        df[new_col] = df[new_col].fillna(0)
        return df
    
    # read in necessary data files for computing features
    to = pd.read_csv(datdir + 'interim/tourney_outcomes.csv')
    c = pd.read_csv(datdir + 'scrub/coaches.csv')

    # merge coach file with team tourney outcomes file
    df = pd.merge(c, to, how='outer', on=['season', 'team_id'])

    # add last day for each team to account for removed coaches
    df['team_last'] = df.groupby(['team_id', 'season'])['last_day'].transform(max)
    
    # if coach wasn't still with team for tourney, recode wins and games
    df['wins'] = np.where(df.last_day != df.team_last, np.nan, df['wins'])
    df['games'] = np.where(df.last_day != df.team_last, np.nan, df['games'])
    
    # if games/wins missing, means no tourney appearance and true value is 0
    df['games'] = df['games'].fillna(0)
    df['wins'] = df['wins'].fillna(0)

    # keep one row per team season
    df = df[df.last_day == df.team_last]
    
    # first sort before creating shifted variables
    df = df.sort_values(['coach_name', 'season'])
    
    # 0/1 indicator of whether coach was in tourney, for creating features
    df['c_vis'] = np.where(df['games'] > 0, 1, 0)

    # indicator of whether coach was in prior year's tourney
    df['c_last'] = df.groupby('coach_name')['c_vis'].apply(shift)
    # if missing, row is coach's first year and value should be 0
    df['c_last'] = np.where(df['c_last'].isnull(), 0, df['c_last'])

    # cvisits as cumulative sum of # of previous tourney visits
    df = create_shift(df, 'c_vis', 'c_visits', cumsum)
    # cfar as the maximum tourney round previously reached
    df = create_shift(df, 'games', 'c_far', cummax)
    # cwon as 0/1 indicator, 1 if coach has won tourney game
    df['c_won'] = np.where(df['c_far'] > 1, 1, 0)
    
    # want indicator for row if coach made elite 8 (round 4)
    # create dict for rounds as keys
    d_round = dict.fromkeys([0, 1, 2, 3], 0)
    # update for rounds 4-6
    d_round.update(d_round.fromkeys([4, 5, 6], 1))
    # create row indicator for elite 8 using dict
    df['round_4'] = df['games'].map(d_round)
    
    # want indicator if coach made final 4 (round 5)
    # update dict with new round 4 value 
    d_round[4] = 0
    # create row indicator for final 4 using dict
    df['round_5'] = df['games'].map(d_round)
    
    # create c_r4sum as cumulative number of elite 8 trips
    df = create_shift(df, 'round_4', 'c_r4sum', cumsum)
    # create c_r5sum as cumulative number of final 4 trips
    df = create_shift(df, 'round_5', 'c_r5sum', cumsum)
    
    # create indicators if coach ever made elite 8 and final 4
    df['c_r4ever'] = np.where(df['c_r4sum'] >=1, 1, 0)
    df['c_r5ever'] = np.where(df['c_r5sum'] >=1, 1, 0)
    
    # indicator of coach having many trips (over 5) without elite 8 trip
    df['c_bit'] = np.where((df['c_visits'] > 5) & (df['c_far'] < 4), 1, 0)
    
    # keep only coach features and unique keys
    cols_drop = ['c_vis', 'wins', 'games', 'round_4', 'round_5', 'first_day',
                 'last_day','team_last', 'coach_name']
    df = df.drop(cols_drop, axis=1)
    
    # no tourney data prior to season 1985, remove rows
    df = df[df['season'] > 1985]
    
    # save data
    data.Clean.write_file(df, datdir + 'features/', 'team_coach')

def tourney_teams(datdir, df):
    """Use seeds data and left join to restrict data to tourney teams."""
    # read in seeds data
    data_in = datdir + '/scrub/seeds.csv'
    left = pd.read_csv(data_in)
    # keep team identifer and season
    merge_on = ['team_id', 'season']
    left = left[merge_on]
    # left join to drop teams not in tourney
    df = pd.merge(left, df, on=merge_on, how='left')
    # return restricted data
    return df

def merge_features(datdir):
    """Combine all data in the features directory."""
    # specify location
    feat_dir = datdir + '/features/'
    # create list of files
    # exclude 'team_features if exists to avoid duplication
    files = data.Clean.list_of_files(feat_dir, tag_drop = 'team_features')
    # read in and create list of data objects
    df_list = [pd.read_csv(x) for x in files]
    # use Clean module to merge all data objects in the list
    merge_on = ['team_id', 'season']
    df = data.Clean.merge_from_list(df_list, merge_on, how='outer')
    # filter to tourney teams
    df = tourney_teams(datdir, df)
    # save file to features directory
    data.Clean.write_file(df, feat_dir, 'team_features', keep_index=False)
