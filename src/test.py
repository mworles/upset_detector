from data import Transfer, Match, Generate, Ratings, Clean
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

#Match.create_key(datdir)


dba = Transfer.DBAssist()
dba.connect()

table = dba.return_table('game_scores')
df = pd.DataFrame(table[1:], columns=table[0])
season = max([x.split('/')[0] for x in df['date'].values])
df['season'] = season

df = Match.id_from_name(datdir, df, 'team_tcp', 'away_team', drop=False)
df = Match.id_from_name(datdir, df, 'team_tcp', 'home_team', drop=False)
df = tcp_conversion(df)
df = Generate.convert_team_id(df, ['wteam', 'lteam'], drop=False)


# add column indicating score for each team
df = Generate.team_scores(df)
# remove games without scores
df = df.sort_index()
df = Generate.team_locations(df)
df = df.dropna(how='any', subset=['t1_score', 't2_score', 't1_loc', 't2_loc'])
df['t1_score'] = df['t1_score'].astype(int)
df['t2_score'] = df['t2_score'].astype(int)
df = Ratings.location_adjustment(df)

tbox = dba.return_table('game_box')

box = pd.DataFrame(tbox[1:], columns=tbox[0])


def split_makes_attempts(elem):
    try:
        elem_split = elem.split('-')
        return (elem_split[0], elem_split[1])
    except:
        return None

for team in ['home_', 'away_']:
    old_col = team + 'FGMA'
    makes_attempts = map(split_makes_attempts, box[old_col].values)
    new_col = team + 'fga'
    box[new_col] = [x[1] if x is not None else None for x in makes_attempts]
    old_col = team + 'FTMA'
    makes_attempts = map(split_makes_attempts, box[old_col].values)
    new_col = team + 'fta'
    box[new_col] = [x[1] if x is not None else None for x in makes_attempts]

# remove missing rows missing any posession cols
pos_list = ['_fta', '_fga', '_OFF', '_TO']
pos_cols = [x for x in box.columns if any(ele in x for ele in pos_list)]
pos_cols = [x for x in pos_cols if not '_TOT' in x]

box = box.dropna(subset=pos_cols, how='any')
bp = box[['gid'] + pos_cols]
bp = bp.set_index('gid').astype(int)

# compute posessions
pos = ((bp['home_fga'] + bp['away_fga']) + 0.475 * 
       (bp['home_fta'] + bp['away_fta']) - 
       (bp['home_OFF'] + bp['away_OFF']) + (bp['home_TO'] + bp['away_TO'])) / 2

bp['pos'] = pos.round(2)
bp = bp.reset_index().drop(columns=pos_cols)
df = pd.merge(df, bp, left_on='gid', right_on='gid', how='inner')
df = df[df['pos'] != 0]
df = Generate.set_gameid_index(df, date_col = 'date', full_date=True, drop_date=False)

df = Ratings.games_by_team(df)
df = Ratings.reduce_margin(df, cap=22)

df['team_off'] = (100 * (df['team_score'] / df['pos'])).round(3)
df['team_def'] = (100 * (df['opp_score'] / df['pos'])).round(3)
df = Ratings.add_weights(df)

udates = pd.unique(df.sort_values('date')['date']).tolist()
date_daynum = {k:v for (k, v) in zip(udates, range(0, len(udates) + 1))}
df['daynum'] = df['date'].map(date_daynum)

keep = ['daynum', 'pos', 'season', 'team_score', 'team_team_id', 'opp_score',
        'opp_team_id', 'team_off', 'team_def', 'weight']
df = df[keep]
df = df.sort_values(['daynum', 'team_team_id'])
df = df.reset_index()

#Clean.write_file(df, datdir + '/interim/', 'games_for_ratings_current', keep_index=True)
#Transfer.create_from_schema('games_for_ratings', 'data/schema.json')
rows = Transfer.dataframe_rows(df)
Transfer.insert('games_for_ratings', rows, at_once=False)
