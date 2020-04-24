import math
import json
import datetime
import pandas as pd
import numpy as np
from src.data.transfer import DBAssist
import ratings
import odds
import spreads
import clean
import generate
import queries
import features


def update_day(date):
    "Run once daily after all games have ended."
    dba = DBAssist()

    # add games for ratings for date
    df = ratings.game_box_for_ratings(date)
    dba.insert_rows('games_for_ratings', df) 
    
    # add stats by team
    mod = "WHERE DATE = '%s'" % (date)
    df = features.team.box_stats_by_team(mod=mod)
    dba.insert_rows('stats_by_team', df, at_once=True)
    
    # convert stats_by_team to stats_by_date
    season = clean.season_from_date(date)
    mod = 'WHERE season = %s' % (season)
    df = dba.return_data('stats_by_team', modifier=mod)
    df = features.team.prep_stats_by_team(df)
    df = features.team.compute_summaries(df)
    dba.insert_rows('stats_by_date', df, at_once=True)
    
    # pull all existing games for ratings from current season
    year = float(date.split('/')[0])
    modifier = "WHERE season = %s" % (str(year))
    df = dba.return_data('games_for_ratings', modifier=modifier)
    year = clean.season_from_date(date)
    df = df[df['season'] == year]
    
    # compute ratings up to date
    df = ratings.run_day(df, n_iters=15)
    # insert current ratings to table
    dba.insert_rows('ratings_at_day', df)
    
    # clean and insert most recent odds for each game on date
    odds = odds.odds_vi(date)
    dba.insert_rows('odds_clean', odds)
    
    # clean and insert most recent spreads for each game on date
    spreads = spreads.spreads_vi(date)
    dba.insert_rows('spreads_clean', spreads)
    
    # insert rows to game_info table for day's games
    mod = "where date = '%s'" % (date)
    df = dba.return_data('game_scores', modifier=mod)
    df = generate.convert_game_scores(df)
    df = generate.make_game_info(df)
    dba.insert_rows('game_info', df)
    
    # get team location for day's games, insert rows to team_home
    rows = generate.game_home(date)
    dba.insert_rows('team_home', rows)

    # get team location for next week's scheduled games
    rows = generate.game_home()
    dba.replace_rows('team_home_scheduled', rows)
    
    dba.close()

def update_current():
    """Run as frequently as desired to update current matchups."""
    dba = DBAssist()

    odds = odds.odds_vi()
    dba.replace_rows('odds_current', odds)

    spreads = spreads.spreads_vi()
    dba.replace_rows('spreads_current', spreads)

    spreads = spreads.drop(columns=['date', 't1_team_id', 't2_team_id'])
    mrg = pd.merge(odds, spreads, left_on='game_id', right_on='game_id',
                   how='outer').reset_index(drop=True)
    mrg = mrg[['game_id', 'date', 't1_team_id', 't2_team_id']]
    dba.replace_rows('matchups_current', mrg)
    
    dba.close()


def current_matchup_features():
    dba = DBAssist()
    df = dba.return_data('matchups_current')

    df = df.sort_values('game_id')
    dates = list(set(df['date']))
    dates = [r"'" + x + r"'" for x in dates]
    dates_in = ", ".join(dates)
    
    modifier = "WHERE date = (SELECT max(date) from ratings_at_day)"
    ratings = dba.return_data('ratings_at_day', modifier=modifier)
    ratings = ratings.drop(columns=['season', 'date'])
    ratings.columns = [x.replace('team_', '') for x in ratings.columns]
    ratings = ratings.rename(columns={'id': 'team_id'})

    modifier = "WHERE date IN (%s)""" % (dates_in)
    home = dba.return_data('team_home', modifier=modifier)
    
    home = home.drop(columns=['game_id'])
    
    df = assign_features(df, ratings)
    df = assign_features(df, home, merge_cols=['date'])
    
    cols_remove = ['game_id', 'date','t1_team_id', 't2_team_id']

    df = df.drop(cols_remove, axis=1).copy()
    
    dba.close()
    
    return df


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
