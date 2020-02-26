from data import Transfer, Generate, Ratings, Match, Odds, Clean, Spreads
import Constants
import pandas as pd
import numpy as np
import math



# create database table
#Transfer.create_from_schema('games_for_ratings', 'data/schema.json')
# create and insert games for ratings up to season 2019
"""
df = Ratings.games_ratings(Constants.DATA)
rows = Transfer.dataframe_rows(df)
Transfer.insert('games_for_ratings', rows, at_once=False)
"""


"""
# clean and insert new games for ratings
#date = "2020/02/19"
df = Ratings.game_box_for_ratings()
rows = Transfer.dataframe_rows(df)
Transfer.insert('games_for_ratings', rows, at_once=False) 
"""

"""
add code to compute historical ratings_at_day
"""


"""
# compute and insert ratings up to most recent date in games_for_ratings
df = Ratings.run_day(year = 2020, n_iters=5)
rows = Transfer.dataframe_rows(df)
#Transfer.create_from_schema('ratings_at_day', 'data/schema.json')
Transfer.insert('ratings_at_day', rows)
"""


"""
# clean from oddsportal and insert
#Transfer.create_from_schema('odds_clean', 'data/schema.json')
datdir = Constants.DATA
df = Odds.clean_oddsportal(datdir)
rows = Transfer.dataframe_rows(df)
Transfer.insert('odds_clean', rows, at_once=False) 
"""


"""
# clean and insert current odds
df = Odds.odds_vi(date=None)
rows = Transfer.dataframe_rows(df)
Transfer.insert('odds_clean', rows, at_once=False) 
"""


"""
# create blended clean spreads table in mysql
datdir = Constants.DATA
df = Spreads.blend_spreads(datdir)
Transfer.create_from_schema('spreads_clean', 'data/schema.json')
rows = Transfer.dataframe_rows(df)
Transfer.insert('spreads_clean', rows, at_once=False) 
"""


"""
# clean and insert current spreads
df = Spreads.spreads_vi(date=None)
rows = Transfer.dataframe_rows(df)
Transfer.insert('odds_clean', rows, at_once=False) 
"""

#Transfer.create_from_schema('ratings_at_day', 'data/schema.json')

if __name__ == '__main__':
    Ratings.run_years(n_iters=15)
