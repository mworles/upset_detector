from data import Transfer, Match, Generate
import pandas as pd
import numpy as np
import Constants
datdir = Constants.DATA


def tcp_conversion(df):
    home_won = df['home_score'] > df['away_score']
    df['wteam'] = np.where(home_won, df['home_team_id'], df['away_team_id'])
    df['lteam'] = np.where(home_won, df['away_team_id'], df['home_team_id'])
    df['wscore'] = np.where(home_won, df['home_score'], df['away_score'])
    df['lscore'] = np.where(home_won, df['away_score'], df['home_score'])
    df['wloc'] = np.where(home_won, 'H', 'A')
    df['wloc'] = np.where(df['neutral'] == 1, 'N', df['wloc'])
    return df


dba = Transfer.DBAssist()
dba.connect()
table = dba.return_table('game_scores')
df = pd.DataFrame(table[1:], columns=table[0])
df = Match.id_from_name(datdir, df, 'team_tcp', 'away_team', drop=False)
df = Match.id_from_name(datdir, df, 'team_tcp', 'home_team', drop=False)
df = tcp_conversion(df)
df = Generate.convert_team_id(df, ['wteam', 'lteam'], drop=False)


# create unique game identifier and set as index
df = Generate.set_gameid_index(df, date_col = 'date', full_date=True, drop_date=False)
# add column indicating score for each team
df = Generate.team_scores(df)
df = df.sort_index()
df = Generate.team_locations(df)

ic = list(df.columns).index('wscore')
print df.iloc[0:20, ic:]

'''
datdir = Constants.DATA
dfn = Generate.neutral_games(datdir)
print dfn.head()
'''
