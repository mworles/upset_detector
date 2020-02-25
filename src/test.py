from data import Transfer, Generate, Ratings, Match, Odds, Clean, Spreads
import Constants
import pandas as pd
import numpy as np

#Transfer.create_from_schema('team_key', 'data/schema.json')
#key = pd.read_csv(Constants.DATA + 'interim/team_key.csv')
#key = key.fillna('')
#key = key.drop_duplicates()

#rows = Transfer.dataframe_rows(key)
#Transfer.insert('team_key', rows, at_once=False) 
"""
# connect to mysql
dba = Transfer.DBAssist()
dba.connect()

# create database table
Transfer.create_from_schema('games_for_ratings', 'data/schema.json')

# insert rows up to season 2019
#df = Ratings.games_ratings(Constants.DATA)
#rows = Transfer.dataframe_rows(df)
#Transfer.insert('games_for_ratings', rows, at_once=False)    

# insert current season rows
df = Ratings.game_box_for_ratings()
rows = Transfer.dataframe_rows(df)
Transfer.insert('games_for_ratings', rows, at_once=False) 

# close msyql connection
dba.close()
"""
#Transfer.create_from_schema('ratings_at_day_test', 'data/schema.json')
"""
date = "2020/02/19"
df = Ratings.game_box_for_ratings(date=date)
rows = Transfer.dataframe_rows(df)
Transfer.insert('games_for_ratings', rows, at_once=False) 
df = Ratings.run_day(n_iters=5)
"""
"""
Transfer.create_from_schema('odds_clean', 'data/schema.json')
datdir = Constants.DATA
df = Odds.clean_oddsportal(datdir)
rows = Transfer.dataframe_rows(df)

Transfer.insert('odds_clean', rows, at_once=False) 

dba = Transfer.DBAssist()
dba.connect()
df = dba.return_df('odds')
    
df = Odds.current_odds(df)
df = Match.id_from_name(df, 'team_vi_odds', 'team_1', drop=False)
df = Match.id_from_name(df, 'team_vi_odds', 'team_2', drop=False)

l_games = Odds.get_odds_dicts(df)
df = Generate.convert_team_id(df, ['team_1_id', 'team_2_id'], drop=False)
df = Odds.team_odds(df, l_games)

years = map(lambda x: x.split('-')[0], df['timestamp'].values)
dates = ["/".join([x, y]) for x, y in zip(years, df['game_date'].values)]
df['date'] = dates
df = Generate.set_gameid_index(df, date_col='date', full_date=True, drop_date=False)
keep_cols = ['date', 't1_team_id', 't2_team_id', 't1_odds', 't2_odds']
df = df[keep_cols].sort_values('date').reset_index()

rows = Transfer.dataframe_rows(df)
Transfer.insert('odds_clean', rows, at_once=False) 
"""
#Match.create_key(Constants.DATA)
datdir = Constants.DATA
df = Spreads.spreads_sbro(datdir)
"""
df = pd.read_csv(datdir + 'external/sbro/spreads_sbro.csv')


df = Match.id_from_name(df, 'team_sbro', 'away', drop=False)
df = Match.id_from_name(df, 'team_sbro', 'home', drop=False)

df['fav_loc'] = np.where(df['favorite'] == df['home'], 'H', 'A')
df['fav_id'] = np.where(df['fav_loc'] == 'H', df['home_id'], df['away_id'])
df = Generate.convert_team_id(df, ['home_id', 'away_id'], drop=False)


def spread_format(x):
    try:
        spread = float(x)
    except:
        spread = np.nan
    return spread
    
df['spnum'] = map(spread_format, df['spread'].values)
df['over_under'] = map(spread_format, df['total'].values)

df['t1_spread'] = np.where(df['t1_team_id'] == df['fav_id'], -df['spnum'], df['spnum'])
df = Generate.set_gameid_index(df, date_col='date', full_date=True, drop_date=False)
keep_cols = ['date', 't1_team_id', 't2_team_id', 't1_spread', 'over_under']
df = df[keep_cols].reset_index()
df = df.sort_values(['date', 't1_team_id'])

print df.describe()
#Transfer.create_from_schema('spreads_clean', 'data/schema.json')
#rows = Transfer.dataframe_rows(df)
#Transfer.insert('spreads_clean', rows, at_once=False) 
"""
