import Transfer, Ratings, Odds, Spreads, Clean, Generate
from features import Create
import pandas as pd
import numpy as np
import math
import json
import datetime
import features

def create(schema_file):
    with open(schema_file, 'r') as f:
        schema = json.load(f)
    for k in schema.keys():
        try:
            Transfer.create_from_schema(k, schema_file)
        except Exception as E:
            print E

def build(datdir, ratings=False):
    """Run once to insert data gathered from flat files."""
    
    # pre-process raw data
    Clean.scrub_files(Constants.RAW_MAP, out='mysql')
    
    df = Ratings.games_ratings(datdir)
    rows = Transfer.dataframe_rows(df)
    Transfer.insert('games_for_ratings', rows, at_once=False)
    
    df = Ratings.game_box_for_ratings()
    rows = Transfer.dataframe_rows(df)
    Transfer.insert('games_for_ratings', rows, at_once=False) 

    df = Transfer.return_data('games_for_ratings')
    years = pd.unique(df['season']).tolist()
    
    if ratings != False:
        if __name__ == '__main__':
            for year in years:
                Ratings.run_year(year=year, n_iters=15)
    
    # clean from oddsportal and insert
    df = Odds.clean_oddsportal(datdir)
    rows = Transfer.dataframe_rows(df)
    Transfer.insert('odds_clean', rows, at_once=False) 

    # create blended clean spreads table
    df = Spreads.blend_spreads(datdir)
    rows = Transfer.dataframe_rows(df)
    Transfer.insert('spreads_clean', rows, at_once=False) 

    # get team home indicators
    # stored results
    for table in ['reg_results', 'ncaa_results', 'nit_results']:
        df = Transfer.return_data(table)
        df = Create.results_home(df)
        results = Transfer.dataframe_rows(df)
        Transfer.insert('team_home', results, at_once=False, create=False,
                        delete=False)

    # scraped results
    results = Create.game_home(date=None)
    Transfer.insert('team_home', results, at_once=False, create=False,
                    delete=False)

    # team info for past seasons
    df = Generate.convert_past_games()
    df = Generate.make_game_info(df)
    rows = Transfer.dataframe_rows(df)
    Transfer.insert('game_info', rows, at_once=False)    #

    
def update_day(date):
    """Run once daily after all games have ended."""
    # add games for ratings for date
    df = Ratings.game_box_for_ratings(date)
    rows = Transfer.dataframe_rows(df)
    Transfer.insert('games_for_ratings', rows, at_once=False) 
    
    # pull all existing games for ratings from current season
    year = float(date.split('/')[0])
    modifier = "WHERE season = %s" % (str(year))
    df = Transfer.return_data('games_for_ratings', modifier=modifier)
    year = Clean.season_from_date(date)
    df = df[df['season'] == year]
    
    # compute ratings up to date
    df = Ratings.run_day(df, n_iters=15)
    # insert current ratings to table
    rows = Transfer.dataframe_rows(df)
    Transfer.insert('ratings_at_day', rows)
    
    # clean and insert most recent odds for each game on date
    odds = Odds.odds_vi(date)
    rows = Transfer.dataframe_rows(odds)
    Transfer.insert('odds_clean', rows, at_once=False)
    
    # clean and insert most recent spreads for each game on date
    spreads = Spreads.spreads_vi(date)
    rows = Transfer.dataframe_rows(spreads)
    Transfer.insert('spreads_clean', rows, at_once=False)
    
    # insert rows to game_info table for day's games
    mod = """where date = '%s'""" % (date)
    df = Transfer.return_data('game_scores', modifier=mod)
    df = Generate.convert_game_scores(df)
    df = Generate.make_game_info(df)
    rows = Transfer.dataframe_rows(df)
    Transfer.insert('game_info', rows, at_once=False)
    
    # get team location for day's games, insert rows to team_home
    rows = features.Create.game_home(date)
    Transfer.insert('team_home', rows, at_once=False)

    # get team location for next week's scheduled games
    start = Clean.date_plus(date, 1)
    end = Clean.date_plus(start, 5)
    dates = Clean.date_range(start, end)
    rows = Create.game_home(dates)
    Transfer.insert("team_home_current", rows, at_once=True, delete=True)

def update_current():
    """Run as frequently as desired to update current matchups."""
    odds = Odds.odds_vi()
    rows = Transfer.dataframe_rows(odds)
    Transfer.insert('odds_current', rows, at_once=False, delete=True)
    
    spreads = Spreads.spreads_vi()
    rows = Transfer.dataframe_rows(spreads)
    Transfer.insert('spreads_current', rows, at_once=False, delete=True)
    
    spreads = spreads.drop(columns=['date', 't1_team_id', 't2_team_id'])
    mrg = pd.merge(odds, spreads, left_on='game_id', right_on='game_id',
                   how='outer').reset_index(drop=True)
    mrg = mrg[['game_id', 'date', 't1_team_id', 't2_team_id']]
    rows = Transfer.dataframe_rows(mrg)
    Transfer.insert('matchups_current', rows, at_once=False, delete=True)

def features_to_matchup(df_mat, df_feat, merge_cols=[]):
    # copy team features and merge for each team in matchup
    # df_feat must have 'team_id' column as team identifer
    t1_merge = ['t1_team_id'] + merge_cols
    t2_merge = ['t2_team_id'] + merge_cols
    
    t1 = df_feat.copy()
    t1.columns = ['t1_' + x if x not in merge_cols else x for x in t1.columns]
    t2 = df_feat.copy()
    t2.columns = ['t2_' + x if x not in merge_cols else x for x in t2.columns]

    mrg = pd.merge(df_mat, t1, left_on=t1_merge, right_on=t1_merge, how='inner')
    mrg = pd.merge(mrg, t2, left_on=t2_merge, right_on=t2_merge, how='inner')
    
    return mrg


def current_matchup_features():
    
    df = Transfer.return_data('matchups_current')
    df = df.sort_values('game_id')
    dates = list(set(df['date']))
    dates = [r"'" + x + r"'" for x in dates]
    dates_in = ", ".join(dates)
    
    modifier = "WHERE date = (SELECT max(date) from ratings_at_day)"
    ratings = Transfer.return_data('ratings_at_day', modifier=modifier)
    ratings = ratings.drop(columns=['season', 'date'])
    ratings.columns = [x.replace('team_', '') for x in ratings.columns]
    ratings = ratings.rename(columns={'id': 'team_id'})

    modifier = "WHERE date IN (%s)""" % (dates_in)
    home = Transfer.return_data('team_home', modifier=modifier)
    home = home.drop(columns=['game_id'])
    
    df = features_to_matchup(df, ratings)
    df = features_to_matchup(df, home, merge_cols=['date'])
    
    cols_remove = ['game_id', 'date','t1_team_id', 't2_team_id']

    df = df.drop(cols_remove, axis=1).copy()
    
    return df

def update_day_tmp(date):
    # pull all existing games for ratings from current season
    year = float(date.split('/')[0])
    modifier = "WHERE season = %s" % (str(year))
    df = Transfer.return_data('games_for_ratings', modifier=modifier)
    year = Clean.season_from_date(date)
    df = df[df['season'] == year]
    
    # use only games occuring up to date
    df = df[df['date'] <= date]
    
    # compute ratings up to date
    df = Ratings.run_day(df, n_iters=15)
    # insert current ratings to table
    rows = Transfer.dataframe_rows(df)
    Transfer.insert('ratings_at_day', rows)
    
    # insert rows to game_info table for day's games
    mod = """where date = '%s'""" % (date)
    df = Transfer.return_data('game_scores', modifier=mod)
    df = Generate.convert_game_scores(df)
    df = Generate.make_game_info(df)
    rows = Transfer.dataframe_rows(df)
    Transfer.insert('game_info', rows, at_once=False)
    
    # get team location for day's games, insert rows to team_home
    rows = features.Create.game_home(date)
    Transfer.insert('team_home', rows, at_once=False)

    # get team location for upcoming games
    start = Clean.date_plus(date, 1)
    end = Clean.date_plus(start, 5)
    dates = Clean.date_range(start, end)
    rows = Create.game_home(dates)
    Transfer.insert("team_home_current", rows, at_once=True, delete=True)
