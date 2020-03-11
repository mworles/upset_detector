import Transfer
import Ratings
import Odds
import Spreads
import Clean
import Generate
import pandas as pd
import numpy as np
import math
import json
import datetime
import queries

def create(schema_file):
    with open(schema_file, 'r') as f:
        schema = json.load(f)
    for k in schema.keys():
        try:
            Transfer.create_from_schema(k, schema_file)
        except Exception as E:
            print E

def matchups_bet(mat):
    # make table of odds/spreads specific to games in matchups
    s = Transfer.return_data('spreads_clean')

    s = s.dropna(subset=['t1_spread'])
    s = s[['game_id', 't1_spread', 'over_under']]

    mrg1 = pd.merge(mat, s, how='left', left_on='game_id', right_on='game_id')

    o = Transfer.return_data('odds_clean')
    o = o[['game_id', 't1_odds', 't2_odds', 't1_odds_dec', 't2_odds_dec']]

    mrg2 = pd.merge(mrg1, o, how='left', left_on='game_id', right_on='game_id')

    cols_bet = ['game_id', 'season', 'date', 't1_team_id', 't2_team_id',
                't1_spread', 'over_under', 't1_odds', 't2_odds', 't1_odds_dec',
                't2_odds_dec']

    df = mrg2[cols_bet].copy()
    
    return df

def get_fav_dog(gd):
    t1 = gd['t1_team_id']
    t2 = gd['t2_team_id']
    
    if math.isnan(gd['t1_spread']) or gd['t1_spread'] == 0:
        if gd['odds_val'] == 0:
            # if spread and both odds missing, use efficiency difference
            if gd['t1_eff_diff'] < 0:
                un_fav = (t1, t2)
            else:
                un_fav = (t2, t1)
        elif gd['odds_val'] == 1:
            if math.isnan(gd['t1_odds']):
                # if only have t2 odds, t2 is favorite if negative, otw underdog
                if gd['t2_odds'] < 0:
                    un_fav = (t1, t2)
                else:
                    un_fav = (t2, t1)
            else:
                # if only have t1 odds, t1 is favorite if negative, otw underdog
                if gd['t1_odds'] < 0:
                    un_fav = (t2, t1)
                else:
                    un_fav = (t1, t2)
        else:
            # if have both odds, team w/ larger value is underdog
            if gd['t1_odds_dec'] > gd['t2_odds_dec']:
                un_fav = (t1, t2)
            else:
                un_fav = (t2, t1)
    else:
        # if have t1_spread and positive, t1 is underdog
        if gd['t1_spread'] > 0:
            un_fav = (t1, t2)
        else:
            un_fav = (t2, t1)
        
    return un_fav

def set_fav_dog(mat):

    mat = mat[['game_id', 'season', 'date', 't1_eff_marg', 't2_eff_marg']]
    
    mb = Transfer.return_data('matchups_bet')
    mb = mb.drop(columns=['season', 'date', 'over_under'])

    df = pd.merge(mat, mb, how='inner', left_on='game_id', right_on='game_id')

    df['t1_eff_diff'] = df['t1_eff_marg'] - df['t2_eff_marg']

    df['odds_val'] = df[['t1_odds', 't2_odds']].count(axis=1)

    cols_for_dict = ['t1_team_id', 't2_team_id', 't1_spread', 't1_odds',
                     't2_odds', 't1_odds_dec', 't2_odds_dec', 't1_eff_diff', 'odds_val']

    dr = df[cols_for_dict].to_dict('records')

    un_fav = map(get_fav_dog, dr)

    df['t_under'] = [x[0] for x in un_fav]
    df['t_favor'] = [x[1] for x in un_fav]

    df = df.drop(columns=['t1_eff_diff', u'odds_val'])

    df = df[['game_id', 'season', 'date', 't_under', 't_favor']]
    
    return df

def columns_by_team(df, col_names):
    # when data has columns for two teams
    # split and create single table with one row per team
    all_cols = ['game_id', 'date', 'team_id']
    all_cols.extend(col_names)
    for col in col_names:
        t1_cols = all_cols[0:2] + ['t1_' + x for x in all_cols[2:]]
        t2_cols = all_cols[0:2] + ['t2_' + x for x in all_cols[2:]]
    t1 = df[t1_cols].copy()
    t1.columns = all_cols
    t2 = df[t2_cols].copy()
    t2.columns = all_cols
    both = pd.concat([t1, t2], sort=False)
    both = both.sort_values('game_id')
    return both

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
    odds = Odds.odds_table()
    rows = Transfer.dataframe_rows(odds)
    Transfer.insert('odds_clean', rows, at_once=True)

    # create table of odds by game_id and team
    df = Transfer.return_data('odds_clean')
    col_names = ['odds', 'odds_dec']
    cbt = columns_by_team(df, col_names)
    Transfer.insert_df('odds_by_team', cbt, at_once=True, create=True)

    # create blended clean spreads table
    df = Spreads.blend_spreads(datdir)
    # move game_id to column
    df = df.reset_index()
    rows = Transfer.dataframe_rows(df)
    Transfer.insert('spreads_clean', rows, at_once=False) 

    df = Transfer.return_data('spreads_clean')
    df['t2_spread'] = - df['t1_spread']
    col_names = ['spread']
    sbt = columns_by_team(df, col_names)
    Transfer.insert_df('spreads_by_team', sbt, at_once=True, create=True)
    
    # get team home indicators
    # stored results
    for table in ['reg_results', 'ncaa_results', 'nit_results']:
        df = Transfer.return_data(table)
        df = Generate.results_home(df)
        results = Transfer.dataframe_rows(df)
        Transfer.insert('team_home', results, at_once=False, create=False,
                        delete=False)
    
    # scraped results
    results = Generate.game_home(date=None)
    Transfer.insert('team_home', results, at_once=False, create=False,
                    delete=False)
    
    # create game info for past seasons
    df = Generate.convert_past_games()
    df = Generate.make_game_info(df)
    rows = Transfer.dataframe_rows(df)
    Transfer.insert('game_info', rows, at_once=False)

    # create game info for scraped scores
    df = Transfer.return_data('game_scores')
    df = Generate.convert_game_scores(df)
    df = Generate.make_game_info(df)
    rows = Transfer.dataframe_rows(df)
    Transfer.insert('game_info', rows, at_once=True)
    
    # game scores, win, and margin by team
    df = Transfer.return_data('game_info')
    df['t2_win'] = np.where(df['t1_win'] == 1, 0, 1)
    df['t2_marg'] = - df['t1_marg']
    col_names = ['score', 'win', 'marg']
    cbt = columns_by_team(df, col_names)
    Transfer.insert_df('results_by_team', cbt, at_once=True, create=True)
    
def transform():
    # create table of all game stats with row per team
    # regular season results
    reg_dtl = Transfer.return_data('reg_results_dtl')
    reg_com = Transfer.return_data('reg_results')
    reg_com = reg_com[reg_com['season'] < reg_dtl['season'].min()]
    df = pd.concat([reg_dtl, reg_com], sort=False)
    df = Generate.games_by_team(df)    
    Transfer.insert_df('stats_by_team', df, create=True, at_once=False) 

    # ncaa tourney game results
    dtl = Transfer.return_data('ncaa_results_dtl')
    com = Transfer.return_data('ncaa_results')
    com_keep = com[com['season'] < dtl['season'].min()]
    df = pd.concat([dtl, com_keep], sort=False)
    sbt = Generate.games_by_team(df)
    Transfer.insert_df('stats_by_team', sbt, at_once=True) 

def modify():
    dba = Transfer.DBAssist()
    dba.connect()
    table_name = "ratings_at_day"
    schema = dba.table_schema(table_name)
    result = dba.run_query(queries.ratings_t1)
    table = dba.table_rows(result, schema)

    result = dba.run_query(queries.ratings_t2)
    t2 = dba.table_rows(result, schema)

    table.extend(t2[1:])

    Transfer.insert('ratings_needed', table, at_once=True, create=False,
                    delete=False)
    
    # create matchups table with game targets and features
    mod = "WHERE season >= 2003"
    mat = Transfer.return_data("game_info", modifier=mod)
    
    rat = Transfer.return_data("ratings_needed")
    rat = rat.drop('season', axis=1)

    df = Updater.assign_features(mat, rat, merge_cols=['date'])
    
    # select dates with ratings features
    mod = "WHERE date > '2002/10/01'"
    home = Transfer.return_data('team_home', modifier=mod)
    home = home.drop('game_id', axis=1)
    df = Updater.assign_features(df, home, merge_cols=['date'])
    rows = Transfer.dataframe_rows(df)
    Transfer.insert('matchups', rows, at_once=True, create=True,
                    delete=True)
    
    # merge matchups with spreads and odds, create table
    mat_bet = Builder.matchups_bet(df)
    rows = Transfer.dataframe_rows(mat_bet)
    Transfer.insert('matchups_bet', rows, create=True, at_once=True)
    
    # set favorite and underdog for all matchups
    df = Transfer.return_data('matchups', modifier=None)
    fav_dog = set_fav_dog(df)
    Transfer.insert_df('fav_dog', fav_dog, at_once=True, create=True,
                       delete=False)

    df = Transfer.return_data('seeds')
    # obtain the integer value from string seed
    df['seed'] = df['seed'].apply(Clean.get_integer)
    Transfer.insert_df('team_seeds', df, at_once=True, create=True,
                       delete=False)