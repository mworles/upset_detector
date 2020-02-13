import pandas as pd
import numpy as np
import Constants
import os
import data

def get_location(row):
    "Returns string indicator of game location for team."
    lteam_dict = {'A': 'H', 'H': 'A', 'N': 'N'}
    if row[0] == row[2]:
        return row[1]
    else:
        return lteam_dict[row[1]]

def team_locations(loc_mat):
    "Given matrix of winner id, winner location, and team id, returns vector of game locations."
    team_loc = map(lambda x: get_location(x), loc_mat)
    return team_loc 

"""
# read in data file with game results
datdir = Constants.DATA
files = data.Clean.list_of_files(datdir + 'scrub/', tag = 'results_dtl')
df_list = [pd.read_csv(x) for x in files]

# combine all games to one dataset
df = pd.concat(df_list, sort=False)

# import and merge seasons for dates
s = pd.read_csv(datdir + 'scrub/seasons.csv')
df = pd.merge(df, s, on='season', how='inner')

# add string date column to games
df['date_id'] = df.apply(data.Clean.game_date, axis=1)

# create outcome-neutral team identifier
df = data.Generate.convert_team_id(df, ['wteam', 'lteam'], drop=False)
# create unique game identifier and set as index
df = data.Generate.set_gameid_index(df, full_date=True, drop_date=False)

# add column indicating score for each team
scores = data.Generate.team_scores(df)

# matrix for t1_teams
t1_mat = scores[['wteam', 'wloc', 't1_team_id']].values
t2_mat = scores[['wteam', 'wloc', 't2_team_id']].values

scores['t1_loc'] = team_locations(t1_mat)
scores['t2_loc'] = team_locations(t2_mat)

scores = scores.sort_index()

# adjust scores for team location
adjust_dict = {'A': 1.875, 'H': -1.875, 'N':0}
t1_adjust = map(lambda x: adjust_dict[x], scores['t1_loc'].values)
t2_adjust = map(lambda x: adjust_dict[x], scores['t2_loc'].values)

scores['t1_score'] = scores['t1_score'] + t1_adjust
scores['t2_score'] = scores['t2_score'] + t2_adjust

# estimate possessions per team
pos = ((df['wfga'] + df['lfga']) + 0.475 * (df['wfta'] + df['lfta']) - 
       (df['wor'] + df['lor']) + (df['wto'] + df['lto'])) / 2
df['pos'] = pos

keep = ['season', 'daynum', 't1_team_id', 't2_team_id', 't1_score', 't2_score',
        'pos']
df = df[keep]

# duplicate so each team has one row per game
t2 = df.copy()
cols = list(df.columns)
change_dict = {'t1_': 't2_', 't2_': 't1_'}
t2_cols = []

for col in cols:
    if any(x in col for x in change_dict.keys()):
        pref = [x for x in change_dict.keys() if x in col][0]
        result = col.replace(pref, change_dict[pref])
    else:
        result = col
    t2_cols.append(result)

t2.columns = t2_cols
all = pd.concat([df, t2], sort=True)

# estimate points per 100 possessions
all['t1_off'] = 100 * (all['t1_score'] / all['pos'])
all['t1_def'] = 100 * (all['t2_score'] / all['pos'])


cols_rename = [x.replace('t1_', 'team_') for x in all.columns]
cols_rename = [x.replace('t2_', 'opp_') for x in cols_rename]

all.columns = cols_rename

all.to_pickle('my_df.pickle')
"""

def create_team_dict(df, col_name, group_by='opp'):
    team_dict = {}
    
    if group_by == 'team':
        dict_cols = ['opp_team_id'] + [col_name]
    else:
        dict_cols = ['team_team_id'] + [col_name]
    
    gb = df.groupby(group_by + '_team_id')
    
    for g, d in gb:
        team_dict[g] = d[dict_cols].values

    return team_dict

def opponent_mean(team_opp, team_dict):
    opponents_scores = [x[1] for x in team_dict[team_opp[1]] if x[0] != team_opp[0]]
    opponents_mean = np.mean(opponents_scores)
    return opponents_mean

def add_grand_mean(df, colname, on_day=True):
    
    new_col = colname + '_gm'
    # drop the old grand mean from data if it exists
    if new_col in df.columns:
        df = df.drop(new_col, axis=1)

    if on_day == True:
        gm = df.groupby(['daynum'])[colname].mean().reset_index()
        gm[new_col] = gm[colname]
        gm = gm[['daynum', new_col]]
        df = pd.merge(df, gm[['daynum', new_col]], on='daynum', how='inner')
    else:    
        df[new_col] = df[colname].mean()
    
    return df

def adjust_tempo(df, id_array, input='adjusted'):
    adjust_col = 'pos'
    if input == 'adjusted':
        adjust_col += '_adj'
    
    team_dict = create_team_dict(df, adjust_col, group_by='team')
    df['opp_pos_mn'] = map(lambda x: opponent_mean(x, team_dict), id_array)
    df['team_pos_mn'] = df.groupby('team_team_id')[adjust_col].transform('mean')
    team_pos_diff = df['team_pos_mn'] - df['pos_gm']
    opp_pos_diff = df['opp_pos_mn'] - df['pos_gm']
    exp_diff = df['pos'] - (df['pos_gm'] + team_pos_diff + opp_pos_diff)
    pos_adj_pct = exp_diff / (df['team_pos_mn'] + df['opp_pos_mn'])
    df['pos_adj'] = df['team_pos_mn'] + (df['team_pos_mn'] * pos_adj_pct)
    df = df.drop(columns=['team_pos_mn', 'opp_pos_mn'])
    return df

def adjust_offense(df, id_array, input='adjusted'):
    if input == 'adjusted':
        adjust1_col = 'team_def_adj'
        adjust1_grp = 'team'
        adjust1_dem = 'opp_def_adj'
    else:
        adjust1_col = 'team_off'
        adjust1_grp = 'opp'
        adjust1_dem = 'opp_def'
        
    # offensive efficiency
    # keys are team ids
    # values 2d matrix with opponents' opponent id and points allowed
    team_dict = create_team_dict(df, adjust1_col, group_by=adjust1_grp)
    # mean points allowed by opponents in games with other teams
    df[adjust1_dem] = map(lambda x: opponent_mean(x, team_dict), id_array)
    # adjust offense to grand mean and defense
    df['team_off_adj'] = (df['team_off'] * df['team_off_gm']) / df[adjust1_dem]
    
    return df

def adjust_defense(df, id_array):
    # keys are team ids
    # values 2d matrix with opponent id and points scored
    team_dict = create_team_dict(df, 'team_off_adj', group_by='team')
    # mean points scored by opponent in games with other teams
    df['opp_off_adj'] = map(lambda x: opponent_mean(x, team_dict), id_array)
    # adjust to grand mean and offense
    df['team_def_adj'] = (df['team_def'] * df['team_def_gm']) / df['opp_off_adj']

    return df

def adjusted_scores(df, input='adjusted'):
    # compute grand means
    df = add_grand_mean(df, 'team_off')
    df = add_grand_mean(df, 'team_def')
    df = add_grand_mean(df, 'pos', on_day=False)

    id_array = df[['team_team_id', 'opp_team_id']].values
    
    df = adjust_offense(df, id_array, input=input)
    df = adjust_defense(df, id_array)
    df = adjust_tempo(df, id_array, input=input)

    return df

def get_schedule_strength(df_games, df_teams):
    dfsub = df_games[['daynum', 'team_team_id', 'opp_team_id']]
    dfsub = pd.merge(dfsub, df_teams, left_on=['opp_team_id'],
                     right_on=['team_id'], how='inner')
    sos = dfsub.groupby(['team_id'])['eff_marg'].median()
    sos = sos.reset_index()
    sos = sos.rename(columns={'team_team_id': 'team_id', 'eff_marg': 'sos_adj'})
    return sos


def condense_df(df):

    ratings = ['team_off_adj', 'team_def_adj', 'pos_adj']

    df_teams = df.groupby('team_team_id')[ratings].mean().reset_index()

    df_teams  = df_teams.rename(columns = {'team_team_id': 'team_id'})
    
    df_teams['eff_marg'] = df_teams['team_off_adj'] + (100 - df_teams['team_def_adj'])

    sos = get_schedule_strength(df, df_teams)
    
    df_teams = pd.merge(df_teams, sos, on=['team_id'])

    return df_teams

def run(data, year):
    
    df = data[data['season'] == year].copy()
    
    dfrun = df.copy()
    
    result = {}
    n_iters = 25
    n = 0
    
    while n < n_iters:
        print """
        """
        print 'iteration %s' % (n)
        
        if n == 0:
            input = 'raw'
        else:
            input = 'adjusted'
        
        df_adjusted = adjusted_scores(dfrun, input=input)
        df_teams = condense_df(df_adjusted)
        print df_teams.describe()
        
        dfrun = df_adjusted.copy()
        
        n += 1
    
    return df_teams

df = pd.read_pickle('my_df.pickle')
df_teams = run(df, 2018)

print df_teams.sort_values('eff_marg', ascending=False)
