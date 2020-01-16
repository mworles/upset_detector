import pandas as pd
import numpy as np
import data.Clean



def get_coach(dir):
    shift = lambda x: x.shift(1)
        
    def create_shift(df, col, new_col, func):
        df[new_col] = df.groupby('coach_name')[col].apply(func)
        df[new_col] = df.groupby('coach_name')[new_col].apply(shift)
        df[new_col] = df[new_col].fillna(0)
        return df
    
    # read in data files
    to = pd.read_csv(dir + 'interim/tourney_outcomes.csv')
    c = pd.read_csv(dir + 'scrub/coaches.csv')

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
    
    data.Clean.write_file(df, dir + 'features/', 'coach')
