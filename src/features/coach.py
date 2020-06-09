""" coach.

A module for computing features associated with coaches. Features computed
for each unique team season to represent past results for team's current coach.

Functions
---------
run
    Top-level function to return features associated with team's coaches.

tourney_features
    Return features on historical tourney performance for coach's.

year_indicators
    Return indicators on tournament performance for each year.

career_indicators
    Return indicators on cumulative tournament performance for a coach
    throughout their full career.

cumulative_sum
    Return cumulative sum of values for a group.

cumulative_max
    Return cumulative max of values for a group.

shift_down
    Return value shifted down one row for a group.

"""
import numpy as np
import pandas as pd
from src.data.transfer import DBAssist

def run(modifier=None):
    """
    Return dataframe with team identifiers and features related to historical
    performance of team's current coach.

    Parameters
    ----------
    modifier : str
        Modifier for MySQL query to pull coach data from coaches table.

    Returns
    -------
    df : pandas DataFrame
        Contains team id, season, and coach features.

    """
    # import coach season data
    dba = DBAssist()
    df = dba.return_data('coaches')

    # handle rare cases of multiple rows for same coach, team, and season
    df = df.sort_values(['team_id', 'last_day'])
    duplicated = df[['team_id', 'season', 'coach_name']].duplicated()
    df = df[~duplicated]

    team_season = df.groupby(['team_id', 'season'])
    team_last = team_season['last_day'].transform(max)
    coaches_end = df[df['last_day'] == team_last]
    coaches_end = coaches_end.drop(['first_day', 'last_day'], axis=1)
    
    # get tourney outcomes for coaches who made the tourney
    tourney_results = dba.return_data('tourney_success')
    tourney_outcomes = pd.merge(coaches_end, tourney_results, how='inner',
                                on=['season', 'team_id'])

    # merge back with all coaches
    merge_on = ['coach_name', 'season', 'team_id']
    df = pd.merge(df, tourney_outcomes, how='left', on=merge_on)
    # if games or wins missing, no tourney appearance and true value is 0
    df['games'] = df['games'].fillna(0)
    df['wins'] = df['wins'].fillna(0)

    # remove columns not needed for tourney features
    df = df.drop(['team_id', 'first_day', 'last_day'], axis=1)
    # compute features of career performance from past tourneys
    tf = tourney_features(df)

    # merge tourney features with all unique coach, season combos
    merge_to = coaches_end[['season', 'coach_name', 'team_id']]
    merge_on = ['coach_name', 'season']
    coach_features = pd.merge(merge_to, tf, how='left', on=merge_on)

    # no tourney data prior to season 1985, remove rows
    coach_features = coach_features[coach_features['season'] > 1985]

    # any missing values indicate coach's first season, tourney features all 0
    coach_features = coach_features.fillna(0)
    
    # keep identifers (team, season) and numeric features only
    coach_features = coach_features.drop(['coach_name'], axis=1)

    # close database connection
    dba.close()

    return coach_features


def tourney_features(df):
    """
    Return dataframe with coach names and features related to historical
    performance in the championship tournament.

    Parameters
    ----------
    df : pandas DataFrame
        Modifier for MySQL query to pull coach data from coaches table.

    Returns
    -------
    df : pandas DataFrame
        Contains team id, season, and coach features.

    """
    df = year_indicators(df)
    df = career_indicators(df)
    
    # remove the yearly indicators not used as features
    df = df.drop(['games', 'wins', 'round_4', 'round_5'], axis=1)

    # shift features by one row so they reflect past performance
    not_features = ['season', 'coach_name']
    features = [col for col in df.columns if col not in not_features]
    for column in features:
        df[column] = df.groupby('coach_name')[column].apply(shift_down)

    # use more accurate/desriptive label for making prior year's tourney
    df = df.rename(columns={'made': 'coach_inlast'})

    return df


def year_indicators(df):
    """
    Return df with indicators of tourney achievements for individual years.

    Parameters
    ----------
    df : pandas DataFrame
        Must contain 'games' column indicating number of tourney games for
        coach each year.

    Returns
    -------
    df : pandas DataFrame
        The input data with columns added for annual indicators of tourney
        performance.

    """
    # 0/1 indicator of whether coach was in tourney
    df['made'] = np.where(df['games'] > 0, 1, 0)

    # create round map to indicate if coach made at least round 4 ("elite 8")
    round_map = dict.fromkeys([0, 1, 2, 3], 0)
    round_map.update(dict.fromkeys([4, 5, 6], 1))
    df['round_4'] = df['games'].map(round_map)

    # update map and create column for making at least round 5 ("final 4")
    round_map[4] = 0
    df['round_5'] = df['games'].map(round_map)

    return df


def career_indicators(df):
    """
    Return data with aggregate career indicators of tourney achievements for
    coaches.

    Parameters
    ----------
    df : pandas DataFrame
        Must contain 'games' and 'made' columns to compute career indicators,
        and 'coach' and 'season' columns to identify unique coach years.

    Returns
    -------
    df : pandas DataFrame
        The input data with columns added for career indicators of tourney
        performance.

    """
    # make sure no coaches in same season have same name
    # future-proof against potential exceptions
    unique_cols = ['coach_name', 'season']
    duplicated = df[df[unique_cols].duplicated()]
    names = duplicated[unique_cols].values
    n_duplicated = duplicated.shape[0]
    assert(n_duplicated == 0), "{} duplicated but not allowed".format(names)

    # use chronological order for cumulative features
    df = df.sort_values(['coach_name', 'season'])
    coach_group = df.groupby('coach_name')

    # num of visits, furthest round, and num of times in elite 8 and final 4
    df['coach_visits'] = coach_group['made'].apply(cumulative_sum)
    df['coach_far'] = coach_group['games'].apply(cumulative_max)
    df['r4times'] = coach_group['round_4'].apply(cumulative_sum)
    df['r5times'] = coach_group['round_5'].apply(cumulative_sum)

    # binary features for ever been, won a game, ever made elite 8 or final 4
    df['coach_any'] = np.where(df['coach_visits'] != 0, 1, 0)
    df['coach_won'] = np.where(df['coach_far'] > 1, 1, 0)
    df['r4made'] = np.where(df['r4times'] >=1, 1, 0)
    df['r5made'] = np.where(df['r5times'] >=1, 1, 0)

    # many trips without reaching at least elite 8 (called "snakebit")
    is_bit = (df['coach_visits'] > 5) & (df['coach_far'] < 4)
    df['bit'] = np.where(is_bit, 1, 0)

    return df


def cumulative_sum(value):
    """Return the cumulative sum value for a group."""
    return value.cumsum()


def cumulative_max(value):
    """Return the cumulative maximum value for a group."""
    return value.cummax()


def shift_down(value):
    """Return value for a group shifted down one row."""
    return value.shift(1)
