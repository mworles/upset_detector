from data import Transfer, Match, Generate, Ratings
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
"""
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
# remove games without scores
df = df.sort_index()
df = Generate.team_locations(df)
df = df.dropna(how='any', subset=['t1_score', 't2_score', 't1_loc', 't2_loc'])
df['t1_score'] = df['t1_score'].astype(int)
df['t2_score'] = df['t2_score'].astype(int)
df = Ratings.location_adjustment(df)
"""
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
pos_list = [x for x in pos_list if not '_TOT' in x]
pos_cols = [x for x in box.columns if any(ele in x for ele in pos_list)]
print pos_cols
box = box.dropna(subset=pos_cols, how='any')


# need to merge in box scores


'''

    df = compute_posessions(df)
    keep = ['season', 'daynum', 't1_team_id', 't2_team_id', 't1_score', 't2_score',
            'pos']
    df = df[keep]
    df = games_by_team(df)
    df = reduce_margin(df, cap=22)

    # compute points per 100 possessions
    df['team_off'] = (100 * (df['team_score'] / df['pos'])).round(3)
    df['team_def'] = (100 * (df['opp_score'] / df['pos'])).round(3)
    df = add_weights(df)
    Clean.write_file(df, datdir + '/interim/', 'games_for_ratings', keep_index=True)
    return df
'''
