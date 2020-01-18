import pandas as pd

datdir = '../../data/'

df = pd.read_csv(datdir + 'interim/games.csv', index_col=0)
spreads = pd.read_csv(datdir + 'interim/spreads.csv', index_col=0)
odds = pd.read_csv(datdir + 'interim/odds.csv', index_col=0)

spreads = spreads.drop(['t1_team_id', 't2_team_id'], axis=1)
df = pd.merge(df, spreads, how='left', left_index=True, right_index=True)

odds = odds.drop(['t1_team_id', 't2_team_id'], axis=1)
df = pd.merge(df, odds, how='left', left_index=True, right_index=True)
