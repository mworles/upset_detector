import numpy as np

def create_shift(df, col, new_col, func):
    """General function used to create new column and shift value."""
    # group by coach and apply given function on given column
    df[new_col] = df.groupby('coach_name')[col].apply(func)
    # shift values down by one row, indicators reflect past performance
    df[new_col] = df.groupby('coach_name')[new_col].apply(shift)
    # any null values should be treated as 0
    df[new_col] = df[new_col].fillna(0)
    return df

# create functions to use with grouped data
# shift row value down
shift = lambda x: x.shift(1)
# computes cumulative sum
cumsum = lambda x: x.cumsum()
# computes cumulative max
cummax = lambda x: x.cummax()

def tourney_success(df):
    """Create data containing coach features."""
    
    # add 1 to season to make indicators represent past experience
    df['season'] = df['season'] + 1

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
    df = create_shift(df, 'round_4', 'c_r4times', cumsum)
    # create c_r5sum as cumulative number of final 4 trips
    df = create_shift(df, 'round_5', 'c_r5times', cumsum)
    
    # create indicators if coach ever made elite 8 and final 4
    df['c_r4ever'] = np.where(df['c_r4times'] >=1, 1, 0)
    df['c_r5ever'] = np.where(df['c_r5times'] >=1, 1, 0)
    
    # indicator of coach having many trips (over 5) without elite 8 trip
    df['c_bit'] = np.where((df['c_visits'] > 5) & (df['c_far'] < 4), 1, 0)
    
    # keep only coach features and unique keys
    cols_drop = ['c_vis', 'wins', 'games', 'round_4', 'round_5', 'first_day',
                 'last_day','team_last', 'coach_name']
    df = df.drop(cols_drop, axis=1)
    
    # no tourney data prior to season 1985, remove rows
    df = df[df['season'] > 1985]
    
    return df
