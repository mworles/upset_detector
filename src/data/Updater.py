import Transfer, Ratings, Odds, Spreads
import Constants
import pandas as pd
import numpy as np
import math
import json



def create(schema_file):
    with open(schema_file, 'r') as f:
        schema = json.load(f)
    for k in schema.keys():
        try:
            Transfer.create_from_schema(k, 'data/schema.json')
        except Exception as E:
            print E

def build(datdir, ratings=False):
    """Run once to insert data gathered from flat files."""
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

def update_day(date):
    """Run once daily after all games have ended."""
    # add games for ratings for date
    df = Ratings.game_box_for_ratings(date)
    rows = Transfer.dataframe_rows(df)
    Transfer.insert('games_for_ratings', rows, at_once=False) 
    
    # pull all exising games for ratings from current season
    year = float(date.split('/')[0])
    modifier = "WHERE season = %s" % (str(year))
    df = Transfer.return_data('games_for_ratings', modifier=modifier)
    year = float(date.split('/')[0])
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
