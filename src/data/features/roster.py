from src.data import Transfer
import pandas as pd
import numpy as np
import math

def height_inches(x):
    '''Returns inches from string height.'''
    try:
        f = int(x.split('-')[0])
        i = int(x.split('-')[1])
        return (f * 12) + i
    except:
        return None


def convert_to_numeric(df, columns):
    # convert each column to numeric
    for c in columns:
        df[c] = df[c].astype(float)
    return df

def get_starters(group):
    grp = group.sort_values('min_pg', ascending=False)
    grp['starter'] = 'no'
    grp.iloc[0:5, 0] = 'yes'
    return grp
    
def weight_minutes(row, weight_var):
    "#Return list of player's height repeated n for n minutes played.#"
    # integer of player's average minutes per game
    min = round(row['min_pg'], 0)
    # 
    if math.isnan(min):
        return []
    else:
        wv = row[weight_var]
        return [wv] * int(min)

def team_weighted(df, weight_var, select=None):
    "#Returns team average of minutes-adjusted height.#"
    # obtain single list of "height minutes" for all players
    hm = np.array([h for ph in df[weight_var].values for h in ph])
    
    if select == None:
        hm_select = hm

    else:
        # divide height minutes into fifths, one for each position
        hm5 = np.array_split(hm, 5)
        # empty array to add select groups
        hm_select = np.array([])
        
        for group in select:
            hm_select = np.append(hm_select, hm5[group])

        #hm_select = np.concatenate(tuple(), axis=None)
    return round(hm_select.mean(), 2)


def game_score(row):
    gs = (row['pts_pg'] + (0.4 * row['fgm_pg']) - (0.7 * row['fga_pg']) - 
          (0.4 * (row['fta_pg'] - row['ftm_pg'])) + (0.7 * row['rbo_pg']) +
          (0.3 * row['rbd_pg']) + row['stl_pg'] + (0.7 * row['ast_pg']) + 
          (0.7 * row['blk_pg']) - (0.4 * row['pf_pg']) - row['to_pg'])
    return gs

"""
# read roster info and player per game stats
tr = Transfer.return_data('team_roster')
pg = Transfer.return_data('player_pergame')

#temp write to pickle file
tr.to_pickle('tr.pkl')
pg.to_pickle('pg.pkl')


#temp read from pickle file
ros = pd.read_pickle('tr.pkl')
ppg = pd.read_pickle('pg.pkl')

# merge rows for season, team, and player
merge_on = ['season', 'team', 'name']
df = pd.merge(ros, ppg, how='inner', left_on=merge_on, right_on=merge_on)

#temp write to pickle file
df.to_pickle('df0.pkl')
"""
#temp read from pickle file
df = pd.read_pickle('df0.pkl')

# exclude seasons before 2002, b/c of missing data
df = df[df['season'] >= 2002]

# calculate height to numeric inches, for computing team features
df['height_num'] = map(height_inches, df['height'].values)

convert_cols = ['min_pg', 'ast_pg', 'to_pg', 'g', 'pts_pg', 'g_start', 'fgm_pg',
                'fga_pg', 'fta_pg', 'ftm_pg', 'rbo_pg', 'rbd_pg', 'stl_pg',
                'ast_pg', 'blk_pg', 'pf_pg', 'to_pg']
df = convert_to_numeric(df, convert_cols)

# clean positon categories
pos_map = {'C': 'F', 'F': 'F', 'G': 'G', 'F-C': 'F', 'PF': 'F', 'PG': 'G', 
           'SF': 'F', 'SG': 'G'}
df['position'] = df['position'].map(pos_map)


# identify starters
# temp column for computing ordinal ranks
df['one'] = 1
# sort by minutes and create minutes rank column
df = df.sort_values(['season', 'team', 'min_pg'],
                    ascending=[True, True, False])
df['min_rnk'] = df.groupby(['season', 'team'])['one'].transform('cumsum')

# sort by games_started and minutes rank, compute combined rank
df = df.sort_values(['season', 'team', 'g_start', 'min_rnk'],
                    ascending=[True, True, False, True])
df['gs_rnk'] = df.groupby(['season', 'team'])['one'].transform('cumsum')

# player is starter if in first 5 of team's sorted rows
df['starter'] = np.where(df['gs_rnk'] < 6, 'starter', 'bench')

# apply consistent formatting and replace nulls 
df['class'].fillna('', inplace=True)
df['class'] = map(str.upper, df['class'])

# numeric experience from class
expdict = {'SR': 3, 'JR': 2, 'SO': 1, 'FR': 0}
df['yrs_exp'] = df['class'].map(expdict)

# compute player 'game score', a numeric index of individual player performance
#df['game_score'] = df.apply(game_score, axis=1)

# herfindahl index, a measure of scoring balance
df['pts_tm'] = df.groupby(['season', 'team'])['pts_pg'].transform('sum').values
pts_zip = zip(df['pts_pg'].values, df['pts_tm'].values)
rat_squared = lambda x: (x[0] / x[1]) ** 2
df['pr_squared'] = map(rat_squared, pts_zip)
hhi = df.groupby(['season', 'team'])['pr_squared'].sum().reset_index()
hhi = hhi.rename(columns={'pr_squared': 'hhi_pts'})

hhi = hhi.sort_values('hhi_pts', ascending=False)

"""
# remove players w/ less than 4 minutes per game
# assume these are not significant contributors
#hm = df[df['min_pg'] >= 4.0]

# order tallest players first
hm = hm.sort_values(['season', 'team', 'height_num'], ascending=False)
# returns list of "height minutes" for each row
hm['height_min'] = hm.apply(height_minutes, axis=1)

# create effective height, minutes-adjusted height for 2 tallest positions
si = [0, 1]
height_eff = hm.groupby(['season', 'team']).apply(lambda x: team_heights(x, si))
height_eff = height_eff.reset_index()
height_eff = height_eff.rename(columns={0: 'height_eff'})

# average height, minutes-adjusted height for all positions
height_avg = hm.groupby(['season', 'team']).apply(lambda x: team_heights(x))
height_avg = height_avg.reset_index()
height_avg = height_avg.rename(columns={0: 'height_avg'})

# experience weighted by minutes
hm['exp_min'] = hm.apply(lambda x: weight_minutes(x, 'yrs_exp'), axis=1)
weighted_exp= lambda x: team_weighted(x, 'exp_min')
exp_weight = hm.groupby(['season', 'team']).apply(weighted_exp)
exp_weight = exp_weight.reset_index()
exp_weight = exp_weight.rename(columns={0: 'wexp'})


# assist: TO ratio for guards
go = df[df['position'] == 'G'].copy()
go['ast'] = go['ast_pg'] * go['g']
go['to'] = go['to_pg'] * go['g']
go_atr = go.groupby(['team', 'season'])['ast', 'to'].sum().reset_index()
go_atr['asttrorat_bck'] = (go_atr['ast'] / go_atr['to']).round(3)
go_atr = go_atr.drop(['ast', 'to'], axis=1)

# position scoring
pos_scoring = df.groupby(['team', 'season', 'position'])['pts_pg']
pos_scoring = pos_scoring.sum().unstack('position').reset_index()
pos_scoring['ptspct_g'] = pos_scoring['G'] / (pos_scoring['G'] + pos_scoring['F'])
pos_scoring['ptspct_f'] = 1 - pos_scoring['ptspct_g']
pos_scoring = pos_scoring.drop(['G', 'F'], axis=1)
pos_scoring['ptsrat_g']  = pos_scoring['ptspct_g'] / pos_scoring['ptspct_f']

# starter and bench scoring
sb = df.groupby(['team', 'season', 'starter'])['pts_pg']
sb = sb.sum().unstack('starter').reset_index()
sb['ptsrat_start'] = sb['starter'] / sb['bench'] 
sb['ptspct_st'] = sb['starter'] / (sb['bench'] + sb['starter'])
sb['ptspct_bn'] = 1 - sb['ptspct_st']
sb = sb.drop(['bench', 'starter'] , axis=1)

# average years experience for starters
exp_start = df.groupby(['team', 'season', 'starter'])
exp_start = exp_start['yrs_exp'].mean().unstack('starter').reset_index()
exp_start.drop('bench' , inplace=True, axis=1)
exp_start = exp_start.rename(columns={"starter": "exp_starters"})


"""





"""
# class group, upper or lower
classdict = {'SR': 'upper', 'JR': 'upper', 'SO': 'lower', 'FR': 'lower'}
df['class_grp'] = df['class'].map(classdict)
"""
