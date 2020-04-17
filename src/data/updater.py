import transfer
import ratings
import odds
import spreads
import clean
import generate
import pandas as pd
import numpy as np
import math
import json
import datetime
import queries
import features
import clean

def update_day(date):
    "Run once daily after all games have ended."
    # add games for ratings for date
    df = ratings.game_box_for_ratings(date)
    rows = transfer.dataframe_rows(df)
    transfer.insert('games_for_ratings', rows, at_once=False) 
    
    # add stats by team
    mod = "WHERE DATE = '%s'" % (date)
    df = features.team.box_stats_by_team(mod=mod)
    transfer.insert_df('stats_by_team', df, at_once=True)
    
    # convert stats_by_team to stats_by_date
    season = clean.season_from_date(date)
    mod = 'WHERE season = %s' % (season)
    df = transfer.return_data('stats_by_team', modifier=mod)
    df = features.team.prep_stats_by_team(df)
    df = features.team.compute_summaries(df)
    transfer.insert_df('stats_by_date', df, at_once=True)
    
    # pull all existing games for ratings from current season
    year = float(date.split('/')[0])
    modifier = "WHERE season = %s" % (str(year))
    df = transfer.return_data('games_for_ratings', modifier=modifier)
    year = clean.season_from_date(date)
    df = df[df['season'] == year]
    
    # compute ratings up to date
    df = ratings.run_day(df, n_iters=15)
    # insert current ratings to table
    rows = transfer.dataframe_rows(df)
    transfer.insert('ratings_at_day', rows)
    
    # clean and insert most recent odds for each game on date
    odds = odds.odds_vi(date)
    rows = transfer.dataframe_rows(odds)
    transfer.insert('odds_clean', rows, at_once=False)
    
    # clean and insert most recent spreads for each game on date
    spreads = spreads.spreads_vi(date)
    rows = transfer.dataframe_rows(spreads)
    transfer.insert('spreads_clean', rows, at_once=False)
    
    # insert rows to game_info table for day's games
    mod = "where date = '%s'" % (date)
    df = transfer.return_data('game_scores', modifier=mod)
    df = generate.convert_game_scores(df)
    df = generate.make_game_info(df)
    rows = transfer.dataframe_rows(df)
    transfer.insert('game_info', rows, at_once=False)
    
    # get team location for day's games, insert rows to team_home
    rows = generate.game_home(date)
    transfer.insert('team_home', rows, at_once=False)

    # get team location for next week's scheduled games
    rows = generate.game_home()
    transfer.insert("team_home_scheduled", rows, at_once=False, delete=True)
    
def update_current():
    """Run as frequently as desired to update current matchups."""
    odds = odds.odds_vi()
    rows = transfer.dataframe_rows(odds)
    transfer.insert('odds_current', rows, at_once=False, delete=True)
    
    spreads = spreads.spreads_vi()
    rows = transfer.dataframe_rows(spreads)
    transfer.insert('spreads_current', rows, at_once=False, delete=True)
    
    spreads = spreads.drop(columns=['date', 't1_team_id', 't2_team_id'])
    mrg = pd.merge(odds, spreads, left_on='game_id', right_on='game_id',
                   how='outer').reset_index(drop=True)
    mrg = mrg[['game_id', 'date', 't1_team_id', 't2_team_id']]
    rows = transfer.dataframe_rows(mrg)
    transfer.insert('matchups_current', rows, at_once=False, delete=True)

def assign_features(df_mat, df_feat, team='both', merge_on=[], how='inner'):
    # copy team features and merge for each team in matchup
    # df_feat must have 'team_id' column as team identifer
    t1_merge = ['t1_team_id'] + merge_on
    t2_merge = ['t2_team_id'] + merge_on
    
    t1 = df_feat.copy()
    t1.columns = ['t1_' + x if x not in merge_on else x for x in t1.columns]
    t2 = df_feat.copy()
    t2.columns = ['t2_' + x if x not in merge_on else x for x in t2.columns]
    
    if team == 'both':
        mrg = pd.merge(df_mat, t1, left_on=t1_merge, right_on=t1_merge, how=how)
        mrg = pd.merge(mrg, t2, left_on=t2_merge, right_on=t2_merge, how=how)
    elif team == 't1':
        mrg = pd.merge(df_mat, t1, left_on=t1_merge, right_on=t1_merge, how=how)
    else:
        mrg = pd.merge(df_mat, t1, left_on=t2_merge, right_on=t2_merge, how=how)
    return mrg


def current_matchup_features():
    
    df = transfer.return_data('matchups_current')
    df = df.sort_values('game_id')
    dates = list(set(df['date']))
    dates = [r"'" + x + r"'" for x in dates]
    dates_in = ", ".join(dates)
    
    modifier = "WHERE date = (SELECT max(date) from ratings_at_day)"
    ratings = transfer.return_data('ratings_at_day', modifier=modifier)
    ratings = ratings.drop(columns=['season', 'date'])
    ratings.columns = [x.replace('team_', '') for x in ratings.columns]
    ratings = ratings.rename(columns={'id': 'team_id'})

    modifier = "WHERE date IN (%s)""" % (dates_in)
    home = transfer.return_data('team_home', modifier=modifier)
    home = home.drop(columns=['game_id'])
    
    df = assign_features(df, ratings)
    df = assign_features(df, home, merge_cols=['date'])
    
    cols_remove = ['game_id', 'date','t1_team_id', 't2_team_id']

    df = df.drop(cols_remove, axis=1).copy()
    
    return df
    