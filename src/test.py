from data import Transfer, Generate, Ratings, Match, Odds, Clean, Spreads
import Constants
import pandas as pd
import numpy as np
import math
import json


def db_create(schema_file):
    with open(schema_file, 'r') as f:
        schema = json.load(f)
    for k in schema.keys():
        Transfer.create_from_schema(k, 'data/schema.json')

def db_build(datdir, ratings=False):
    df = Ratings.games_ratings(Constants.DATA)
    rows = Transfer.dataframe_rows(df)
    Transfer.insert('games_for_ratings', rows, at_once=False)
    
    df = Ratings.game_box_for_ratings()
    rows = Transfer.dataframe_rows(df)
    Transfer.insert('games_for_ratings', rows, at_once=False) 

    df = pd.concat([df, df])
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

def db_update(date):
    # add games for ratings for date
    df = Ratings.game_box_for_ratings(date)
    rows = Transfer.dataframe_rows(df)
    Transfer.insert('games_for_ratings', rows, at_once=False) 
    
    # pull all exising games for ratings from current season
    df = Transfer.return_data('games_for_ratings')
    year = float(date.split('/')[0])
    df = df[df['season'] == year]
    # compute ratings up to date
    df = Ratings.run_day(df)
    # insert current ratings to table
    rows = Transfer.dataframe_rows(df)
    Transfer.insert('ratings_at_day', rows)

"""
# clean and insert current odds
df = Odds.odds_vi()
rows = Transfer.dataframe_rows(df)
Transfer.insert('odds_current', rows, at_once=False, delete=True)

# clean and insert current spreads
df = Spreads.spreads_vi()
rows = Transfer.dataframe_rows(df)
Transfer.insert('spreads_current', rows, at_once=False, delete=True)
"""
dba = Transfer.DBAssist()
dba.connect()
odds = dba.return_df('odds_current').set_index('game_id')
spreads = dba.return_df('spreads_current').set_index('game_id')
ratings = dba.return_df('ratings_at_day')
most_recent = max(ratings['date'].values)
ratings = ratings[ratings['date'] == most_recent]
ratings = ratings.drop(columns=['season', 'date'])
ratings.columns = [x.replace('team_', '') for x in ratings.columns]
t1 = ratings.copy()
t1.columns = ['t1_' + x for x in t1.columns]
t2 = ratings.copy()
t2.columns = ['t2_' + x for x in t2.columns]

spreads = spreads.drop(columns=['date', 't1_team_id', 't2_team_id'])
mrg = pd.merge(odds, spreads, left_index=True, right_index=True).reset_index()
mrg = pd.merge(mrg, t1, left_on='t1_team_id', right_on='t1_id', left_index=True, 
               how='inner')
mrg = pd.merge(mrg, t2, left_on='t2_team_id', right_on='t2_id', left_index=True, 
               how='inner')
mrg.set_index('game_id')
mrg = Clean.add_team_name(mrg, datdir='../data/')


print mrg[['team_1', 'team_2', 't1_eff_marg', 't2_eff_marg', 't1_spread', 't1_odds', 't2_odds']].head(30)
