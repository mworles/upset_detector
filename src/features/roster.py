import math
import pandas as pd
import numpy as np

def run(df):
    group_on = ['season', 'team']

    # create a baseline dataset of team and season to merge team statistics
    teams = df[group_on].drop_duplicates().copy()

    # first compute all team stats that don't depend on minutes
    # assist: TO ratio for guards
    gb = assist_to_ratio(df, group_on)
    teams = merge_stat(teams, gb)

    # scoring composition by position
    gb = scoring_by_position(df, group_on)
    teams = merge_stat(teams, gb)

    # scoring composition by starters and bench
    gb = scoring_by_starters(df, group_on)
    teams = merge_stat(teams, gb)

    # average years of experience for starters
    gb = starter_experience(df, group_on)
    teams = merge_stat(teams, gb)
    
    # index of player scoring balance
    gb = scoring_balance(df, group_on)
    teams = merge_stat(teams, scoring_balance(df, group_on))

    # best per game "game score" for team
    # an index of quality for team's best player
    gb = group_stat(df, group_on, 'game_score', 'gmsc_max', 'max')
    teams = merge_stat(teams, gb)
    
    # average per game "game score" for starters
    dfs = df[df['starter'] == 'starter'].copy()
    gb = group_stat(dfs, group_on, 'game_score', 'gmsc_start', 'mean')
    teams = merge_stat(teams, gb)

    # get top player tsp_usage value for each team
    gb = group_stat(df, group_on, 'ts_usage', 'tsusg_max', 'max')
    teams = merge_stat(teams, gb)

    # get top player per_min value for each team
    gb = group_stat(df, group_on, 'per_min', 'permin_max', 'max')
    teams = merge_stat(teams, gb)

    # team minutes continuity is sum of players
    gb = group_stat(df, group_on, 'cont', 'min_cont', 'sum')
    # no prior minutes for 2002, remove rows
    gb = gb[gb['season'] > 2002]
    teams = merge_stat(teams, gb, merge_on=['season', 'team'])

    # sum of recruiting index points for players on team
    gb = group_stat(df, group_on, 'rsci_pts', 'recr_sum', 'sum')    
    teams = merge_stat(teams, gb, merge_on=['season', 'team'])

    # end of roster stats that don't depend on minutes

    # remove rows where team has no minutes data
    df = df[df['team_mpnone'] !=1]

    # bench minutes percentage
    gb = bench_minutes(df, group_on)
    teams = merge_stat(teams, gb)

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
    teams = merge_stat(teams, gb)
    
    #  add metric of minutes-adjusted height for all players
    gb = height_adjusted(df, group_on)
    teams = merge_stat(teams, gb)

    # add "experience minutes" list for each row
    exp_minutes = lambda x: weight_minutes(x, 'yrs_exp')
    df['exp_min'] = df.apply(exp_minutes, axis=1)
    
    gb = experience_adjusted(df, group_on)
    teams = merge_stat(teams, gb)
    teams.to_pickle('teams.pkl')

    teams = teams.sort_values(['season', 'team'])
    return teams


def merge_stat(df, stat, merge_on=['season', 'team']):
    teams = pd.merge(df, stat, how='left', left_on=merge_on, right_on=merge_on)
    return teams


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
    dfp['bal_pts'] = list(map(rat_squared, pts_zip))
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
    teams = pd.merge(gb, bm, how='inner', left_on=group_on, right_on=group_on)
    teams['bench_minpct'] = teams['min_bench'] / teams['min_team']
    teams = teams.drop(['min_team', 'min_bench'], axis=1)
    
    teams['minpct_bench'] = teams['bench_minpct'].round(4)

    return teams


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
