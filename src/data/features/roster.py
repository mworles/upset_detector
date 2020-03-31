from src.data import Transfer
import pandas as pd
import numpy as np
import math

def get_minutes(row, ref_dict):
    name = row['name']
    team = row['team']
    season = row['season']
    min = row['min_pg']
    if (season, team) in ref_dict.keys():
        ref = ref_dict[(season, team)]
        if name in ref['name']:
            i = ref['name'].index(name)
            min = ref['min'][i]
        else:
            min = None
    else:
        min = None
    
    return min

def split_and_fill(df, ref_dict):
    has = df[df['min_pg'].notnull()].copy()
    tf = df[df['min_pg'].isnull()].copy()
    fill_minutes = lambda x: get_minutes(x, ref_dict)
    tf['min_pg'] = tf.apply(fill_minutes, axis=1).astype(float)
    all = pd.concat([has, tf])
    all = all.sort_values(['season', 'team'])
    return all
    
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

def team_weighted(df, weighted_stat, select=None):
    "#Returns team average of stat, weighted by minutes#"
    # obtain single list of "height minutes" for all players
    hm = np.array([h for ph in df[weighted_stat].values for h in ph])
    
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


def usage_rate(row, on_floor=True):
    if row['min_pg'] is None:
        ur = None
    elif row['min_pg'] == 0:
        ur = 0
    else:
        if on_floor==True:
            ur = ((row['fga_pg'] + (0.44 * row['fta_pg']) + row['to_pg']) * 
                  (200 / 5) / (row['min_pg'] * (row['team_fga'] + 
                                                (0.44 * row['team_fta']) + 
                                                row['team_to'])))
        else:
            ur = ((row['fga_pg'] + (0.44 * row['fta_pg']) + row['to_pg']) /
                  (row['team_fga'] + (0.44 * row['team_fta']) + row['team_to']))
        
        ur = round(ur, 4)
    return ur

def get_ranking(x):
    try:
        rank = int(x.split(' ')[0])
    except:
        rank = None
    return rank

def player_efficiency(row):
    if row['min_pg'] is None:
        per = None
    elif row['min_pg'] == 0:
        per = 0
    else:    
        per = ((row['fgm_pg'] * 85.910) + 
         (row['stl_pg'] * 53.897) +
         (row['fg3m_pg'] * 51.757) +
         (row['ftm_pg'] * 46.845) +
         (row['blk_pg'] * 39.190) +
         (row['rbo_pg'] * 39.190) +
         (row['ast_pg'] * 34.677) +
         (row['rbd_pg'] * 14.707) -
         (row['pf_pg'] * 17.174) -
         ((row['fta_pg'] - row['ftm_pg']) * 20.091) - 
         ((row['fga_pg'] - row['fgm_pg']) * 39.190) -
         (row['to_pg'] * 53.897)) / row['min_pg']
        
    return per

def merge_stat(df, stat, merge_on=['season', 'team']):
    mrg = pd.merge(df, stat, how='left', left_on=group_on, right_on=group_on)
    return mrg


# read roster info and player per game stats
tr = Transfer.return_data('team_roster')
pg = Transfer.return_data('player_pergame')

# merge rows for season, team, and player
merge_on = ['season', 'team', 'name']
df = pd.merge(tr, pg, how='inner', left_on=merge_on, right_on=merge_on)

# exclude seasons before 2002, b/c of missing data
df = df[df['season'] >= 2002]

# fill minutes from espn data if missing
ep = Transfer.return_data('espn_pergame')

ref_dict = {}
for name, group in ep.groupby(['season', 'team']):
    ref_dict[name] = group[['name', 'min']].to_dict('list')

# fill minutes for subset of rows missing it
df = split_and_fill(df, ref_dict)

# by team, count players missing minutes and players total
df['mp_null'] = df['min_pg'].isnull().astype(int)
df['one'] = 1
df['team_mp_null'] = df.groupby(['season', 'team'])['mp_null'].transform(sum)
df['team_count'] = df.groupby(['season', 'team'])['one'].transform(sum)


# calculate height to numeric inches, for computing team features
df['height_num'] = map(height_inches, df['height'].values)

convert_cols = ['min_pg', 'ast_pg', 'to_pg', 'g', 'pts_pg', 'g_start', 'fgm_pg',
                'fga_pg', 'fta_pg', 'ftm_pg', 'rbo_pg', 'rbd_pg', 'rb_pg',
                'stl_pg', 'ast_pg', 'blk_pg', 'pf_pg', 'fg3m_pg']
df = convert_to_numeric(df, convert_cols)

# clean positon categories
pos_map = {'C': 'F', 'F': 'F', 'G': 'G', 'F-C': 'F', 'PF': 'F', 'PG': 'G', 
           'SF': 'F', 'SG': 'G'}
df['position'] = df['position'].map(pos_map)

# identify starters
# temporary counter column for computing ordinal ranks
df['one'] = 1

# sort by minutes and create minutes rank column
df = df.sort_values(['season', 'team', 'min_pg'],
                    ascending=[True, True, False])
df['min_rnk'] = df.groupby(['season', 'team'])['one'].transform('cumsum')

# sort by games_started and minutes rank, compute combined rank
df = df.sort_values(['season', 'team', 'g_start', 'min_rnk'],
                    ascending=[True, True, False, True])
df['gs_rnk'] = df.groupby(['season', 'team'])['one'].transform('cumsum')

# starters are first 5 of team when sorted by combined gs/minutes rank
df['starter'] = np.where(df['gs_rnk'] < 6, 'starter', 'bench')

# upper case consistency and replace nulls
df['class'].fillna('', inplace=True)
df['class'] = map(str.upper, df['class'])

# numeric experience from class
expdict = {'SR': 3, 'JR': 2, 'SO': 1, 'FR': 0}
df['yrs_exp'] = df['class'].map(expdict)

# compute player 'game score', a numeric index of individual player performance
df['game_score'] = df.apply(game_score, axis=1)

group_on = ['season', 'team']

# create a baseline dataset to merge team statistics
mrg = df[['team', 'season']].drop_duplicates().copy()

group_on = ['season', 'team']

# remove players w/ less than 4 minutes per game
# assume these are not significant contributors
hm = df[df['min_pg'] >= 4.0]

# order tallest players first
hm = hm.sort_values(['season', 'team', 'height_num'], ascending=False)
# returns list of "height minutes" for each row
hm['height_min'] = hm.apply(lambda x: weight_minutes(x, 'height_num'), axis=1)

# create "effective height"
# indicator of minutes-adjusted height for 2 tallest positions
si = [0, 1]
weighted_height = lambda x: team_weighted(x, 'height_min', si)
height_eff = hm.groupby(group_on).apply(weighted_height)
height_eff = height_eff.reset_index().rename(columns={0: 'height_eff'})
mrg = merge_stat(mrg, height_eff)


# average height, minutes-adjusted height for all positions
weighted_height = lambda x: team_weighted(x, 'height_min')
height_avg = hm.groupby(group_on).apply(weighted_height)
height_avg = height_avg.reset_index().rename(columns={0: 'height_avg'})
mrg = merge_stat(mrg, height_avg)


texp = df.copy()
texp = texp[texp['min_pg'] >= 4.0]
# experience weighted by minutes
texp['exp_min'] = texp.apply(lambda x: weight_minutes(x, 'yrs_exp'), axis=1)
weighted_exp= lambda x: team_weighted(x, 'exp_min')
exp_weight = texp.groupby(group_on).apply(weighted_exp)
exp_weight = exp_weight.reset_index().rename(columns={0: 'wexp'})
mrg = merge_stat(mrg, exp_weight)

# assist: TO ratio for guards
grd = df[df['position'] == 'G'].copy()
grd['ast'] = grd['ast_pg'] * grd['g']
grd['to'] = grd['to_pg'] * grd['g']
grd_atr = grd.groupby(group_on)['ast', 'to'].sum().reset_index()
grd_atr['asttrorat_bck'] = (grd_atr['ast'] / grd_atr['to']).round(3)
grd_atr = grd_atr.drop(['ast', 'to'], axis=1)
mrg = merge_stat(mrg, grd_atr)

# position scoring
ps = df.groupby(['team', 'season', 'position'])['pts_pg']
ps = ps.sum().unstack('position').reset_index()
ps['ptspct_back'] = ps['G'] / (ps['G'] + ps['F'])
ps['ptspct_front'] = 1 - ps['ptspct_back']
ps = ps.drop(['G', 'F'], axis=1)
ps['ptsrat_back']  = ps['ptspct_back'] / ps['ptspct_front']
mrg = merge_stat(mrg, ps)

# starter and bench scoring
sb = df.groupby(['team', 'season', 'starter'])['pts_pg']
sb = sb.sum().unstack('starter').reset_index()
sb['ptsrat_start'] = sb['starter'] / sb['bench'] 
sb['ptspct_st'] = sb['starter'] / (sb['bench'] + sb['starter'])
sb['ptspct_bn'] = 1 - sb['ptspct_st']
sb = sb.drop(['bench', 'starter'] , axis=1)
mrg = merge_stat(mrg, sb)

# average years experience for starters
exp_start = df.groupby(['team', 'season', 'starter'])
exp_start = exp_start['yrs_exp'].mean().unstack('starter').reset_index()
exp_start.drop('bench' , inplace=True, axis=1)
exp_start = exp_start.rename(columns={"starter": "exp_starters"})
mrg = merge_stat(mrg, exp_start)

# herfindahl index, a measure of scoring balance
df['pts_tm'] = df.groupby(group_on)['pts_pg'].transform('sum').values
pts_zip = zip(df['pts_pg'].values, df['pts_tm'].values)
rat_squared = lambda x: (x[0] / x[1]) ** 2
df['pr_squared'] = map(rat_squared, pts_zip)
hhi = df.groupby(group_on)['pr_squared'].sum().reset_index()
hhi = hhi.rename(columns={'pr_squared': 'bal_pts'})
mrg = merge_stat(mrg, hhi)

# best per game "game score" for team, index of quality for team's best player
gs = df.groupby(group_on)['game_score'].agg([('gmsc_max', 'max')])
mrg = merge_stat(mrg, gs)

# average per game "game score" for starters
gss = df[df['starter'] == 'starter']
gss = gss.groupby(group_on)['game_score'].agg([('gmsc_start', 'mean')])
gss['gmsc_start'] = gss['gmsc_start'].round(3)
mrg = merge_stat(mrg, gss)


# team bench minutes percentage
# obtain team total minutes accounted for
bm = df.copy()
mt = bm.groupby(group_on)['min_pg'].sum().reset_index()
mt = mt.rename(columns={'min_pg': 'min_team'})

# select bench and get total bench minute
bm = bm[bm['starter'] == 'bench']
bm = bm.groupby(group_on)['min_pg'].sum().reset_index()
bm = bm.rename(columns={'min_pg': 'min_bench'})

bp = pd.merge(mt, bm, how='inner', left_on=group_on, right_on=group_on)
bp['bench_minpct'] = bp['min_bench'] / bp['min_team']
bp = bp.drop(['min_team', 'min_bench'], axis=1)
mrg = merge_stat(mrg, bp)

# compute player minutes percentage
pm = df.copy()
pm['min_team'] = pm.groupby(group_on)['min_pg'].transform('sum')
pm['min_pct'] = pm['min_pg'] / pm['min_team']

# add col for player's min_pct last season
pm = pm.sort_values(['team', 'name', 'season'])
gb_player = ['team', 'name']
pm['min_pct_lag'] = pm.groupby(gb_player)['min_pct'].apply(lambda x: x.shift(1))
pm['min_pct_lag'] = pm['min_pct_lag'].fillna(0)

# player's minutes continuity is min of current and prior seasons
pm['cont'] = pm[['min_pct', 'min_pct_lag']].min(axis=1)
# team minutes continuity is sum of players
min_cont = pm.groupby(['season', 'team'])['cont'].sum().reset_index()
# no prior minutes for 2002, all zeros, remove rows
min_cont = min_cont[min_cont['season'] > 2002]
mrg = merge_stat(mrg, min_cont, merge_on=['season', 'team'])

# sum of recruiting index points for players on team
rsci = df['rsci'].values
df['rsci_rank'] = map(get_ranking, rsci)
df['rsci_pts'] = (101 - df['rsci_rank']) / 10
df['rsci_pts'] = df['rsci_pts'].fillna(0)
recruit = df.groupby(group_on)['rsci_pts'].agg([('recr_sum', 'sum')])
mrg = merge_stat(mrg, recruit, merge_on=['season', 'team'])

tsu = df.copy()
# player true shooting percentage
tsu['trshpct'] = tsu['pts_pg'] / (2 * (tsu['fga_pg'] + (0.44 * tsu['fta_pg'])))

# transformed team sum columns for usage rate calculation
for col in ['fga_pg', 'fta_pg', 'to_pg']:
    team_sum = 'team_' + col
    team_sum = team_sum.replace('_pg', '')
    tsu[team_sum] = tsu.groupby(group_on)[col].transform(sum)

tsu = tsu[tsu['min_pg'] > 4]
tsu['usage_rate'] = tsu.apply(lambda x: usage_rate(x, on_floor=False), axis=1)

tsu['ts_usage'] = (tsu['usage_rate'] / 0.20) * tsu['trshpct']

# get top tsp_usage value for each team
tsu = tsu.groupby(group_on)['ts_usage'].agg([('tsutop', 'max')]).reset_index()
mrg = merge_stat(mrg, tsu)

# create minutes-adjusted player efficiency rating
per = df.copy()
# remove low overall impact players with few minutes
per = per[per['min_pg'] > 4]
per['per'] = per.apply(player_efficiency, axis=1)
per['per_min'] = per['per'] * (per['min_pg'] / 40)

# get top player for each team
team_per = per.groupby(group_on)['per_min'].agg([('pertop', 'max')]).reset_index()
mrg = merge_stat(mrg, team_per)
