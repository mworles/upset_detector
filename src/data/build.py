# python packages
import pandas as pd
import numpy as np
import math
import json
import datetime

# project packages
from src.data.transfer import DBAssist
import ratings
import odds
import spreads
import clean
import generate
from src import features
from src import constants
from src.constants import SCHEMA_FILE

def run(datdir=constants.DATA_DIR):
    """Run once to insert data gathered from flat files."""
    
    # initialize database helper class
    dba = DBAssist()
    
    # pre-process raw data
    scrub_files(constants.RAW_MAP, out='mysql')

    # create all tables listed in schema
    with open(SCHEMA_FILE, 'r') as f:
        schema_tables = json.load(f).keys()

    for table_name in schema_tables:
        dba.create_from_schema(table_name)

    # lattitude & longitude of teams
    df = pd.read_csv(datdir + 'external/kaggle/TeamGeog.csv')
    # add new team locations
    df_add = features.location.update_teams(df, constants.TEAM_CITY_UPDATE)
    df = pd.concat([df, df_add])
    
    dba.create_from_data('team_geog', df)
    dba.insert_rows('team_geog', df)

    # lattitude and longitude of tourney games '85-'17
    df = pd.read_csv(datdir + 'external/kaggle/TourneyGeog.csv')
    dba.create_from_data('tourney_geog', df)
    dba.insert_rows('tourney_geog', df)

    # manually obtained lattitude and longitude of some cities
    df = pd.read_csv(datadir + 'data/external/locations/cities_manual.csv')
    dba.insert_rows('tourney_geog', df)

    # manually obtained lattitude and longitude of some gyms
    df = pd.read_csv(datdir + 'external/locations/gym_manual.csv')
    dba.create_from_data('gym_manual', df)
    dba.insert_rows('gym_manual', df)

    # games needed for ratings
    games = ratings.games_ratings(datdir)
    games_new = ratings.game_box_for_ratings()
    df = [games, games_new]
    dba.insert_rows('games_for_ratings', df)

    # clean from oddsportal and insert
    df = odds.odds_table()
    dba.insert_rows('odds_clean', df)

    # create table of odds by game_id and team
    df = columns_by_team(odds, ['odds', 'odds_dec'])
    dba.create_from_data('odds_by_team', df)
    dba.insert_rows('odds_by_team', df)

    # create blended clean spreads table
    df = spreads.blend_spreads(datdir)
    # move game_id to column
    df = df.reset_index()
    dba.insert_rows('spreads_clean', df) 

    df['t2_spread'] = - df['t1_spread']
    df = columns_by_team(df, ['spread'])
    dba.create_from_data('speads_by_team', df)
    dba.insert_rows('spreads_by_team', df)

    # get team home indicators
    # stored results
    for table in ['reg_results', 'ncaa_results', 'nit_results']:
        df = dba.return_data(table)
        df = generate.results_home(df)
        dba.insert_rows('team_home', df)
    
    # scraped results
    results = generate.game_home(date=None)
    dba.insert_rows('team_home', results)
    
    # create game info for past seasons
    df = generate.convert_past_games()
    df = generate.make_game_info(df)
    dba.insert_rows('game_info', df)

    # create game info for scraped scores
    df = dba.return_data('game_scores')
    df = generate.convert_game_scores(df)
    df = generate.make_game_info(df)
    dba.insert_rows('game_info', df)
    
    # game scores, win, and margin by team
    df = dba.return_data('game_info')
    df['t2_win'] = np.where(df['t1_win'] == 1, 0, 1)
    df['t2_marg'] = - df['t1_marg']
    col_names = ['score', 'win', 'marg']
    df = columns_by_team(df, col_names)
    dba.create_from_data('results_by_team', df)
    dba.insert_rows('results_by_team', df)

    dba.close()

def modify():
    dba = transfer.DBAssist()
    table_name = "ratings_at_day"
    table_columns = dba.table_columns(table_name)
    r1 = dba.query_result(queries.ratings_t1)
    df1 = pd.DataFrame(r1, columns=table_columns)
    r2 = dba.query_result(queries.ratings_t2)
    df2 = pd.DataFrame(r2, columns=table_columns)
    df = pd.concat([df1, df2], sort=False)
    
    dba.create_from_data('ratings_needed', df)
    dba.insert_rows('ratings_needed', df)
    
    # create matchups table with game targets and features
    mod = "WHERE season >= 2003"
    mat = dba.return_data("game_info", modifier=mod)
    
    rat = dba.return_data("ratings_needed")
    rat = rat.drop('season', axis=1)
    
    df = updater.assign_features(mat, rat, merge_cols=['date'])

    # select dates with ratings features
    mod = "WHERE date > '2002/10/01'"
    home = dba.return_data('team_home', modifier=mod)
    home = home.drop('game_id', axis=1)
    df = updater.assign_features(df, home, merge_cols=['date'])
    
    dba.create_from_data('matchups', df)
    dba.insert_rows('matchups', df)

    # merge matchups with spreads and odds, create table
    df = matchups_bet(df)
    dba.create_from_data('matchups_bet', df)
    dba.insert_rows('matchups_bet', df)
    
    # set favorite and underdog for all matchups
    df = dba.return_data('matchups', modifier=None)
    df = set_fav_dog(df)
    dba.create_from_data('fav_dog', df)
    dba.insert_rows('fav_dog', df)

    df = dba.return_data('seeds')
    # obtain the integer value from string seed
    df['seed'] = df['seed'].apply(lambda x: int(re.sub(r'\D+', '', x)))
    dba.create_from_data('team_seeds', df)
    dba.insert_rows('team_seeds', df)
    dba.create_insert('team_seeds', df, at_once=True)


def matchups_bet(mat):
    """Make table of odds/spreads specific to games in matchups."""
    dba = DBAssist()
    s = dba.return_data('spreads_clean')
    dba.close()
    
    s = s.dropna(subset=['t1_spread'])
    s = s[['game_id', 't1_spread', 'over_under']]

    mrg1 = pd.merge(mat, s, how='left', left_on='game_id', right_on='game_id')

    o = dba.return_data('odds_clean')
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
    dba = DBAssist()
    
    mat = mat[['game_id', 'season', 'date', 't1_eff_marg', 't2_eff_marg']]
    
    mb = dba.return_data('matchups_bet')
    dba.close()

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


def transform():
    dba = transfer.DBAssist()
    
    # create table of all game stats with row per team
    # regular season results
    reg_dtl = dba.return_data('reg_results_dtl')
    reg_com = dba.return_data('reg_results')
    reg_com = reg_com[reg_com['season'] < reg_dtl['season'].min()]
    post_dtl = dba.return_data('ncaa_results_dtl')
    post_com = dba.return_data('ncaa_results')
    com_keep = post_com[post_com['season'] < post_dtl['season'].min()]
    df = pd.concat([reg_dtl, reg_com, post_dtl, post_com], sort=False)
    df = df.drop(['wloc', 'numot'], axis=1)
    df = games_by_team(df)
    dba.create_from_data('stats_by_team', df)
    dba.insert_rows('stats_by_team', sbt)

    # convert stats_by_team to stats_by_date
    # stats only exist after 2002
    mod = """WHERE season >= 2003"""
    dfs = dba.return_data('seasons', mod)
    dfs = dfs[['season', 'dayzero']]
    seasons = list(set(dfs['season'].values))
    seasons.sort()
    
    dba.create_from_schema('stats_by_date')
    for season in seasons:
        mod = "WHERE season = %s" % (season)
        df = dba.return_data('stats_by_team', mod)
        df = features.team.prep_stats_by_team(df)
        dates = list(set(df['date']))
        dates.sort()
        
        for date in dates:
        
            df_date = compute_summaries(df, max_date=date)
            df_date['date'] = date
            df_date['season'] = season

            dba.insert_rows('stats_by_date', df_date)

def scrub_file(name, file_map):
    """Returns dataframe identified by file name with column names formatted and
    renamed according to file map. For files located in 'raw' subdirectory.
    """
    # create relative path to file and read data as dataframe
    file = '../data/raw/' + name + '.csv'
    df = pd.read_csv(file)
    
    # if file map has columns to rename, rename them
    if 'columns' in file_map[name].keys():
        df = df.rename(columns=file_map[name]['columns'])
    
    # column names all lower case for consistency across project
    df.columns = df.columns.str.lower()

    # fix unicode text in some team names
    if 'name_spelling' in df.columns:
        df['name_spelling'] = map(lambda x: x.decode('ascii', 'ignore'),
                                  df['name_spelling'].values)
    
    return df


def scrub_files(file_map, out='mysql', subset=[]):
    """Scrubs and writes all files identified in constants file map.

    Arguments
    ----------
    file_map: dictionary
        Must contain key to match file names. Value is a dict that must contain
        'new_name' key paired with value as string of new file name. Dict may 
        contain 'columns' key indicating columns to rename.
    """
    # collect list of all files to process
    files = file_map.keys()
    
    # use subset to restrict file list
    if len(subset) != 0:
        files = [f for f in files if f in subset]

    # scrub and write each file
    for f in files:
        # obtain data with columns reformatted
        df = scrub_file(f, file_map)
        # get table name
        table_name = file_map[f]['new_name']
        # insert into mysql or save csv files
        if out == 'mysql':
            dba = DBAssist()
            dba.create_from_data(table_name, df)
            dba.insert_rows(table_name, df, at_once=True)
            dba.close()
        else:
            data_out = '../data/scrub/'
            write_file(df, data_out, table_name, keep_index=False)


def results_home(df):

    mat = df[['wteam', 'lteam', 'wloc']].values
    location_map = clean.team_location_map(df)
    
    df = clean.date_from_daynum(df)
    df = clean.order_team_ids(df, ['wteam', 'lteam'])
    df = clean.make_game_id(df)
    df = df.set_index('game_id')
    
    location_map = clean.team_location_map(df)
    df = clean.map_teams(df, location_map, 'loc')

    home_dict = {'H': 1, 'A': 0, 'N': 0}

    t1 = df[['date', 't1_team_id', 't1_loc']].copy()
    t1['home'] = t1['t1_loc'].map(home_dict)
    t1 = t1.drop(columns='t1_loc').rename(columns={'t1_team_id': 'team_id'})

    t2 = df[['date', 't2_team_id', 't2_loc']].copy()
    t2['home'] = t2['t2_loc'].map(home_dict)
    t2 = t2.drop(columns='t2_loc').rename(columns={'t2_team_id': 'team_id'})

    df = pd.concat([t1, t2], sort=False)
    df = df.sort_index().reset_index()

    return df

def convert_past_games():
    dba = DBAssist()
    df1 = dba.return_data('reg_results')
    df1['game_cat'] = 'regular'

    df2 = dba.return_data('nit_results')
    df2 = df2.rename(columns={'secondarytourney': 'game_cat'})

    df3 = dba.return_data('ncaa_results')
    df3['game_cat'] = 'ncaa'
    
    dba.close()

    df = pd.concat([df1, df2, df3], sort=True)

    df = date_from_daynum(df)

    df['season'] = df['season'].astype(int)
    return df

def make_game_info(df):
    # create team_1 and team_2 id identifer columns
    df = order_team_id(df, ['wteam', 'lteam'])
    df = make_game_id(df)
    df = df.set_index('game_id')
    
    # add column indicating scores and locations for each team
    df = team_scores(df)
    df['t1_win'] = np.where(df['t1_score'] > df['t2_score'], 1, 0)
    df['t1_marg'] = df['t1_score'] - df['t2_score']

    cols_keep = ['season', 'date', 'game_cat', 't1_team_id', 't2_team_id', 't1_score',
                 't2_score', 't1_win', 't1_marg']
    df = df.sort_index()
    df = df[cols_keep].reset_index()
    return df

def convert_game_scores(df):
    df = match.id_from_name(df, 'team_tcp', 'away_team', drop=False)
    df = match.id_from_name(df, 'team_tcp', 'home_team', drop=False)

    df['game_cat'] = "NA"
    df['season'] = map(clean.season_from_date, df['date'].values)
    # convert columns to apply neutral id function
    df = game_score_convert(df)
    return df

def game_home(date=None):
    dba = DBAssist()

    if date is not None:
        mod = """WHERE date = '%s'""" % (date)
        df = dba.return_data('game_scores', modifier=mod)
        df = df.drop(columns=['home_score', 'away_score'])
    else:
        df = dba.return_data('game_scheduled')
    dba.close()

    df = match.id_from_name(df, 'team_tcp', 'away_team', drop=False)
    df = match.id_from_name(df, 'team_tcp', 'home_team', drop=False)
    df = order_team_id(df, ['home_team_id', 'away_team_id'])
    df = make_game_id(df)
    df = df.set_index('game_id')
    rows = tcp_team_home(df)
    
    return rows

def tcp_team_home(df):
    game_id = df.index.values.tolist()
    date = df['date'].values.tolist()

    home_id = df['home_team_id'].values
    home = zip(df['neutral'].values, home_id)
    home_loc = [1 if x[0] == 0 else 0 for x in home]
    away_loc = [0 for x in range(0, len(home))]

    home_comb = zip(game_id, date, home_id, home_loc)
    away_comb = zip(game_id, date, df['away_team_id'].values, away_loc)
    rows = [[a, b, c, d] for a, b, c, d in home_comb]
    rows.extend([[a, b, c, d] for a, b, c, d in away_comb])

    rows.sort(key=lambda x: x[0])
    rows.insert(0, ['game_id', 'date', 'team_id', 'home'])
    return rows

def games_by_team(df):
    
    def parse_game(x, col_map, winner=True):
        gen = x[col_map['gen']].tolist()
        wdata = x[col_map['win']].tolist() 
        ldata = x[col_map['lose']].tolist()
        
        if winner==True:
            data = gen + wdata + ldata
        else:
            data = gen + ldata + wdata
    
        return data

    # remove columns not needed
    wcols = [x for x in df.columns if x[0] == 'w']
    lcols = [x for x in df.columns if x[0] == 'l']

    team_cols = ['team_' + c[1:] for c in wcols]
    opp_cols = ['opp_' + c[1:] for c in lcols]

    new_cols = ['season', 'daynum']
    new_cols.extend(team_cols + opp_cols)

    gcoli = [list(df.columns).index(x) for x in ['season', 'daynum']]
    wcoli = [list(df.columns).index(x) for x in wcols]
    lcoli = [list(df.columns).index(x) for x in lcols]

    col_map = {'gen': gcoli,
               'win': wcoli,
               'lose': lcoli}

    df_array = df.values
    # all games parsed twice to create row for winners and row for losers
    winners = map(lambda x: parse_game(x, col_map, winner=True), df_array)
    losers = map(lambda x: parse_game(x, col_map, winner=False), df_array)
    games = winners + losers

    df = pd.DataFrame(games, columns=new_cols)
    df = df.rename(columns={'team_team': 'team_id', 'opp_team': 'opp_id'})

    df = df.sort_values(['season', 'daynum', 'team_id'])
    
    return df
