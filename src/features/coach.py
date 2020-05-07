""" coach.

A module for computing features associated with team coaches. 

Functions
---------
name
    Description.
    
"""
import numpy as np
import pandas as pd
import team
from src.data.transfer import DBAssist

def run():
    dba = DBAssist()

    # import coach season data
    df = dba.return_data('coaches')
    
    # handle rare case of in-season leave and return
    duplicated = df[['team_id', 'season', 'coach_name']].duplicated()
    df = df[~duplicated]
    
    coach_tourney = coach_finishes(df)

    # merge back with original coaches
    merge_on = ['coach_name', 'season', 'team_id']
    df = pd.merge(df, coach_tourney, how='left', on=merge_on)

    ci = coach_indicators(df)

    # add 1 to season to shift indicators forward
    ci['season'] = ci['season'] + 1
    
    # merge indicators with finishing coaches
    merge_on = ['coach_name', 'season']
    merge_to = df[['season', 'coach_name', 'team_id']]
    
    coach_features = pd.merge(merge_to, ci, how='left', on=merge_on)

    # no tourney data prior to season 1985, remove rows
    coach_features = coach_features[coach_features['season'] > 1985]

    # any missing should be zero
    coach_features = coach_features.fillna(0)
    
    dba.close()

    return coach_features

def coach_indicators(df):
    # make sure no coaches in same season have same name
    unique_cols = ['coach_name', 'season']
    duplicated = df[df[unique_cols].duplicated()]
    names = duplicated[unique_cols].values
    n_duplicated = duplicated.shape[0]
    assert(n_duplicated == 0), "{} duplicated but not allowed".format(names)

    # if games/wins missing, means no tourney appearance and true value is 0
    df['games'] = df['games'].fillna(0)
    df['wins'] = df['wins'].fillna(0)

    # 0/1 indicator of whether coach was in tourney, for creating features
    df['made'] = np.where(df['games'] > 0, 1, 0)

    round_map = dict.fromkeys([0, 1, 2, 3], 0)
    # update for rounds 4-6
    round_map.update(dict.fromkeys([4, 5, 6], 1))
    # create indicator for making elite 8
    df['round_4'] = df['games'].map(round_map)
    round_map[4] = 0
    df['round_5'] = df['games'].map(round_map)

    # sort to create cumulative counts
    df = df.sort_values(['coach_name', 'season'])
    coach_group = df.groupby('coach_name')
    
    df['coach_visits'] = coach_group['made'].apply(cumulative_sum)
    df['coach_never'] = np.where(df['coach_visits'] == 0, 1, 0)
    df['coach_far'] = coach_group['games'].apply(cumulative_max)
    df['coach_won'] = np.where(df['coach_far'] > 1, 1, 0)
    df['r4times'] = coach_group['round_4'].apply(cumulative_sum)
    df['r5times'] = coach_group['round_5'].apply(cumulative_sum)
    df['r4made'] = np.where(df['r4times'] >=1, 1, 0)
    df['r5made'] = np.where(df['r5times'] >=1, 1, 0)
    is_bit = (df['coach_visits'] > 5) & (df['coach_far'] < 4)
    df['bit'] = np.where(is_bit, 1, 0)
    
    drop_cols = ['team_id', 'first_day', 'last_day', 'games', 'wins',
                 'round_4', 'round_5']
    df = df.drop(drop_cols, axis=1)
    
    return df


def coach_finishes(df):
    # remove coaches who didn't finish season
    team_season = df.groupby(['team_id', 'season'])
    team_last = team_season['last_day'].transform(max)
    coaches_end = df[df['last_day'] == team_last]
    
    dba = DBAssist()
    # import results from previous tournaments
    df = dba.return_data('ncaa_results')
    dba.close()
    
    # convert results to indicators of team performance in tourney
    tourney_indicators = team.tourney_performance(df)

    coach_tourney = pd.merge(coaches_end, tourney_indicators, how='inner',
                             on=['season', 'team_id'])
    coach_tourney = coach_tourney.drop(['first_day', 'last_day'], axis=1)
    
    return coach_tourney


def cumulative_sum(value):
    return value.cumsum()

def cumulative_max(value):
    return value.cummax()
