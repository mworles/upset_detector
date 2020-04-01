from src.data import Transfer
import pandas as pd
import numpy as np
import math

def fill_minutes(row, ref_dict):
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

def split_and_fill(df):
    has = df[df['min_pg'].notnull()].copy()
    tf = df[df['min_pg'].isnull()].copy()
    
    # import data from espn per_game table
    ep = Transfer.return_data('espn_pergame')
    
    # create dict to look up by (season, team)
    ref_dict = {}
    
    for name, group in ep.groupby(['season', 'team']):
        ref_dict[name] = group[['name', 'min']].to_dict('list')

    fill_rows = lambda x: fill_minutes(x, ref_dict)
    tf['min_pg'] = tf.apply(fill_rows, axis=1).astype(float)
    all = pd.concat([has, tf])
    all = all.sort_values(['season', 'team'])
    
    # convert minutes to float type
    df['min_pg'] = df['min_pg'].astype(float)
    
    
    # by team, count players missing minutes
    df['mp_null'] = df['min_pg'].isnull().astype(int)
    df['team_mp_null'] = df.groupby(['season', 'team'])['mp_null'].transform(sum)
    # count team players
    df['one'] = 1
    df['team_count'] = df.groupby(['season', 'team'])['one'].transform(sum)
    
    # binary indicator, 1 if team missing minutes for all players
    df['team_mpnone'] = np.where(df['team_mp_null'] == df['team_count'], 1, 0)
    
    # remove temporary cols
    df = df.drop(['mp_null', 'one', 'team_mp_null', 'team_count'], axis=1)
    
    return df


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

def starter_indicator(df):
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
    
    # drop temporary columns
    df = df.drop(['one', 'min_rnk', 'gs_rnk'], axis=1)
    
    return df

def experience_indicator(df):
    # upper case consistency and replace nulls
    df['class'].fillna('', inplace=True)
    df['class'] = map(str.upper, df['class'])

    # numeric experience from class
    expdict = {'SR': 3, 'JR': 2, 'SO': 1, 'FR': 0}
    df['yrs_exp'] = df['class'].map(expdict)
    
    return df

def game_score(row):
    gs = (row['pts_pg'] + (0.4 * row['fgm_pg']) - (0.7 * row['fga_pg']) - 
          (0.4 * (row['fta_pg'] - row['ftm_pg'])) + (0.7 * row['rbo_pg']) +
          (0.3 * row['rbd_pg']) + row['stl_pg'] + (0.7 * row['ast_pg']) + 
          (0.7 * row['blk_pg']) - (0.4 * row['pf_pg']) - row['to_pg'])
    return gs


def rsci_score(x):
    try:
        rank = int(x.split(' ')[0])
        pts = (101 - rank) / 10
    except:
        pts = 0
    return pts

def add_rsci_score(df):
    rsci = df['rsci'].values
    #rsci_rank = map(rsci_ranking, rsci)
    #df['rsci_pts'] = (101 - rsci_rank) / 10
    df['rsci_pts'] = map(rsci_score, rsci)
    #df['rsci_pts'] = df['rsci_pts'].fillna(0)
    
    return df

def player_usage(row, on_floor=True):
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

def add_usage(df, on_floor=False):
    
    temp_cols = []
    # add transformed team sum columns for usage rate calculation
    for col in ['fga_pg', 'fta_pg', 'to_pg']:
        team_sum = 'team_' + col
        team_sum = team_sum.replace('_pg', '')
        df[team_sum] = df.groupby(['season', 'team'])[col].transform(sum)
        temp_cols.append(team_sum)

    if on_floor == False:
        col_name = 'usage_raw'
    else:
        col_name = 'usage_rate'

    get_usage = lambda x: player_usage(x, on_floor=on_floor)
    df[col_name] = df.apply(get_usage, axis=1)
    
    df = df.drop(temp_cols, axis=1)
    
    return df

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

def add_efficiency(df):
    # remove low overall impact players with few minutes
    df['per'] = df.apply(player_efficiency, axis=1)
    df['per_min'] = df['per'] * (df['min_pg'] / 40)
    return df

def add_continuity(df):
    # compute player minutes percentage
    df['min_team'] = df.groupby(['season', 'team'])['min_pg'].transform('sum')
    df['min_pct'] = df['min_pg'] / df['min_team']
    
    # add col for player's min_pct last season
    df = df.sort_values(['team', 'name', 'season'])
    gb_player = ['team', 'name']
    df['min_pct_lag'] = df.groupby(gb_player)['min_pct'].apply(lambda x: x.shift(1))
    df['min_pct_lag'] = df['min_pct_lag'].fillna(0)
    
    # player's minutes continuity is min of current and prior seasons
    df['cont'] = df[['min_pct', 'min_pct_lag']].min(axis=1)
    
    drop_cols = ['min_team', 'min_pct', 'min_pct_lag']
    df = df.drop(drop_cols, axis=1)
    return df

def clean_roster(min_season=2002):
    # read roster info and player per game stats
    tr = Transfer.return_data('team_roster')
    pg = Transfer.return_data('player_pergame')

    # merge rows for season, team, and player
    merge_on = ['season', 'team', 'name']
    df = pd.merge(tr, pg, how='inner', left_on=merge_on, right_on=merge_on)

    # drop rows where player_pergame data is missing
    df = df[df['season'] >= min_season]
    
    convert_cols = ['min_pg', 'ast_pg', 'to_pg', 'g', 'pts_pg', 'g_start', 'fgm_pg',
                    'fga_pg', 'fta_pg', 'ftm_pg', 'rbo_pg', 'rbd_pg', 'rb_pg',
                    'stl_pg', 'ast_pg', 'blk_pg', 'pf_pg', 'fg3m_pg']
    df = convert_to_numeric(df, convert_cols)

    # fill missing minutes for rows that have secondary source available
    df['name'] = df['name'].str.lower()
    df = split_and_fill(df)

    # convert string height to numeric inches
    df['height_num'] = map(height_inches, df['height'].values)

    # clean positon categories
    pos_map = {'C': 'F', 'F': 'F', 'G': 'G', 'F-C': 'F', 'PF': 'F', 'PG': 'G', 
               'SF': 'F', 'SG': 'G'}
    df['position'] = df['position'].map(pos_map)

    df = starter_indicator(df)
    df = experience_indicator(df)
    df = add_rsci_score(df)
    df = add_usage(df, on_floor=False)
    df = add_usage(df, on_floor=True)
    df = add_efficiency(df)
    df = add_continuity(df)

    # compute player 'game score', a numeric index of individual player performance
    df['game_score'] = df.apply(game_score, axis=1)
    
    # compute player's true shooting percentage
    df['trshpct'] = df['pts_pg'] / (2 * (df['fga_pg'] + (0.44 * df['fta_pg'])))

    # combined index of raw usage and true schooting
    df['ts_usage'] = (df['usage_raw'] / 0.20) * df['trshpct']

    return df

def merge_stat(df, stat, merge_on=['season', 'team']):
    mrg = pd.merge(df, stat, how='left', left_on=merge_on, right_on=merge_on)
    return mrg

def group_stat(df, group_on, stat_col, new_col, func):
    gb = df.groupby(group_on)[stat_col].agg([(new_col, func)]).reset_index()
    gb[new_col] = gb[new_col].round(4)
    return gb

def assist_to_ratio(df, group_on):
    # restrict data to guards
    dfg = df[df['position'] == 'G'].copy()
    # calculate total assists and turnovers
    dfg['ast'] = dfg['ast_pg'] * dfg['g']
    dfg['to'] = dfg['to_pg'] * dfg['g']
    # compute sums for each team
    gb = dfg.groupby(group_on)['ast', 'to'].sum().reset_index()
    # remove teams missing turnover data
    gb = gb[gb['to'] != 0]
    # calculate and keep assist:turnover ratio
    gb['assto_back'] = (gb['ast'] / gb['to']).round(3)
    gb = gb.drop(['ast', 'to'], axis=1)
    return gb

def scoring_by_position(df, group_on):
    # scoring broken down by position groups
    gb = df.groupby(group_on + ['position'])['pts_pg']
    gb = gb.sum().unstack('position')
    # guard scoring percentage
    gb['ptspct_back'] = gb['G'] / (gb['G'] + gb['F'])
    # forward & center scoring percentage
    gb['ptspct_front'] = 1 - gb['ptspct_back']
    gb = gb.drop(['G', 'F'], axis=1)
    # ratio of guard scoring percentage to forwards/centers
    gb['ptsrat_back']  = gb['ptspct_back'] / gb['ptspct_front']
    
    gb = gb.round(4)
    gb = gb.reset_index()
    
    return gb

def scoring_by_starters(df, group_on):
    # starter and bench scoring
    gb = df.groupby(group_on + ['starter'])['pts_pg']
    gb = gb.sum().unstack('starter')
    # starter scoring percentage
    gb['ptspct_start'] = gb['starter'] / (gb['bench'] + gb['starter'])
    # bench scoring percentage
    gb['ptspct_bench'] = 1 - gb['ptspct_start']
    # ratio of starter scoring to bench
    gb['ptsrat_start'] = gb['ptspct_start'] / gb['ptspct_bench'] 
    gb = gb.drop(['bench', 'starter'] , axis=1)
    
    gb = gb.round(4)
    gb = gb.reset_index()
    
    return gb

def starter_experience(df, group_on):
    dfs = df[df['starter'] == 'starter'].copy()
    gb = dfs.groupby(group_on)['yrs_exp'].agg([('exp_starters', 'mean')])
    gb = gb.round(4)
    gb = gb.reset_index()
    return gb

def scoring_balance(df, group_on):
    df['pts_tm'] = df.groupby(group_on)['pts_pg'].transform('sum')
    # remove players without points
    dfp = df.dropna(subset=['pts_tm', 'pts_pg'], how='any').copy()
    pts_zip = zip(dfp['pts_pg'].values, dfp['pts_tm'].values)
    # herfindahl index, a measure of scoring balance
    rat_squared = lambda x: (x[0] / x[1]) ** 2
    dfp['bal_pts'] = map(rat_squared, pts_zip)
    gb = dfp.groupby(group_on)['bal_pts'].sum()
    gb = gb.round(4)
    gb = gb.reset_index()    
    return gb

def bench_minutes(df, group_on):
    # obtain team total minutes accounted for
    gb = df.groupby(group_on)['min_pg'].sum().reset_index()
    gb = gb.rename(columns={'min_pg': 'min_team'})
    
    # select bench and get total bench minutes
    bm = df[df['starter'] == 'bench'].copy()
    bm = bm.groupby(group_on)['min_pg'].sum().reset_index()
    bm = bm.rename(columns={'min_pg': 'min_bench'})
    
    # merge total and bench minutes
    mrg = pd.merge(gb, bm, how='inner', left_on=group_on, right_on=group_on)
    mrg['bench_minpct'] = mrg['min_bench'] / mrg['min_team']
    mrg = mrg.drop(['min_team', 'min_bench'], axis=1)
    
    mrg['minpct_bench'] = mrg['bench_minpct'].round(4)

    return mrg

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

def height_effective(df, group_on):
    # create "effective height"
    # indicator of minutes-adjusted height for 2 tallest positions
    eff_height = lambda x: team_weighted(x, 'height_min', [0, 1])
    gb = df.groupby(group_on).apply(eff_height)
    gb = gb.round(4)
    gb = gb.reset_index().rename(columns={0: 'height_eff'})
    
    return gb
    
def height_adjusted(df, group_on):
    adj_height = lambda x: team_weighted(x, 'height_min')
    gb = df.groupby(group_on).apply(adj_height)
    gb = gb.round(4)
    gb = gb.reset_index().rename(columns={0: 'height_avg'})
    return gb
    
def experience_adjusted(df, group_on):
    adj_exp = lambda x: team_weighted(x, 'exp_min')
    gb = df.groupby(group_on).apply(adj_exp)
    gb = gb.round(4)
    gb = gb.reset_index().rename(columns={0: 'exp_wghtd'})
    return gb

def run():
    #df = clean_roster(min_season=2002)
    df = pd.read_pickle('df.pkl')
    group_on = ['season', 'team']
    
    # create a baseline dataset of team and season to merge team statistics
    mrg = df[group_on].drop_duplicates().copy()

    # first compute all team stats that don't depend on minutes
    # assist: TO ratio for guards
    gb = assist_to_ratio(df, group_on)
    mrg = merge_stat(mrg, gb)

    # scoring composition by position
    gb = scoring_by_position(df, group_on)
    mrg = merge_stat(mrg, gb)

    # scoring composition by starters and bench
    gb = scoring_by_starters(df, group_on)
    mrg = merge_stat(mrg, gb)

    # average years of experience for starters
    gb = starter_experience(df, group_on)
    mrg = merge_stat(mrg, gb)
    
    # index of player scoring balance
    gb = scoring_balance(df, group_on)
    mrg = merge_stat(mrg, scoring_balance(df, group_on))

    # best per game "game score" for team
    # an index of quality for team's best player
    gb = group_stat(df, group_on, 'game_score', 'gmsc_max', 'max')
    mrg = merge_stat(mrg, gb)
    
    # average per game "game score" for starters
    dfs = df[df['starter'] == 'starter'].copy()
    gb = group_stat(dfs, group_on, 'game_score', 'gmsc_start', 'mean')
    mrg = merge_stat(mrg, gb)

    # get top player tsp_usage value for each team
    gb = group_stat(df, group_on, 'ts_usage', 'tsusg_max', 'max')
    mrg = merge_stat(mrg, gb)

    # get top player per_min value for each team
    gb = group_stat(df, group_on, 'per_min', 'permin_max', 'max')
    mrg = merge_stat(mrg, gb)

    # team minutes continuity is sum of players
    gb = group_stat(df, group_on, 'cont', 'min_cont', 'sum')
    # no prior minutes for 2002, remove rows
    gb = gb[gb['season'] > 2002]
    mrg = merge_stat(mrg, gb, merge_on=['season', 'team'])

    # sum of recruiting index points for players on team
    gb = group_stat(df, group_on, 'rsci_pts', 'recr_sum', 'sum')    
    mrg = merge_stat(mrg, gb, merge_on=['season', 'team'])

    # end of roster stats that don't depend on minutes

    # remove rows where team has no minutes data
    df = df[df['team_mpnone'] !=1]

    # bench minutes percentage
    gb = bench_minutes(df, group_on)
    mrg = merge_stat(mrg, gb)

    # remove players w/ less than 4 minutes per game
    # assumed to not be significant contributors
    df = df[df['min_pg'] >= 4.0]

    # order tallest players first
    df = df.sort_values(['season', 'team', 'height_num'], ascending=False)
    # add "height minutes" list for each row
    height_minutes = lambda x: weight_minutes(x, 'height_num')
    df['height_min'] = df.apply(height_minutes, axis=1)

    # add metric of minutes-adjusted height for team's tallest players
    gb =  height_effective(df, group_on)
    mrg = merge_stat(mrg, gb)
    
    #  add metric of minutes-adjusted height for all players
    gb = height_adjusted(df, group_on)
    mrg = merge_stat(mrg, gb)

    # add "experience minutes" list for each row
    exp_minutes = lambda x: weight_minutes(x, 'yrs_exp')
    df['exp_min'] = df.apply(exp_minutes, axis=1)
    
    gb = experience_adjusted(df, group_on)
    mrg = merge_stat(mrg, gb)
    mrg.to_pickle('mrg.pkl')

    mrg = mrg.sort_values(['season', 'team'])
    return mrg

df = run()
#df = pd.read_pickle('mrg.pkl')
Transfer.insert_df('roster_features', df, create=True, at_once=True)
