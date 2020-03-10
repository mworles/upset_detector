from src.data import Transfer
import pandas as pd
import numpy as np


# code to use on team game row data
def compute_game_stats(df):
    # estimated possessions per team in game
    df['team_win'] = np.where(df['team_score'] > df['opp_score'], 1, 0)
    df['team_loss'] = np.where(df['team_score'] < df['opp_score'], 1, 0)

    df['pos'] = ((df.team_fga + df.opp_fga) +
                 0.44 * (df.team_fta + df.opp_fta) -
                 (df.team_or + df.opp_or) +
                 (df.team_to + df.opp_to)) / 2
    # scoring margin
    df['scrmarg'] = df.team_score - df.opp_score
    # scoring margin percentage
    df['scrmargpct'] = (df.team_score - df.opp_score) / df.opp_score
    # score ratio
    df['scrrat'] = df.team_score / df.opp_score
    # free throw shooting percentage`
    df['ftpct'] = df.team_ftm / df.team_fta
    # free throw rate
    df['ftrat'] = df.team_fta / df.team_fga
    # free throw made rate
    df['ftmrat'] = df.team_ftm / df.team_fga
    # free throws made per possession
    df['ftm_adpos'] = df.team_ftm / df.pos
    # defensive free throw rate
    df['ftrat_d'] = df.opp_fta / df.opp_fga
    # effective field goal percentage
    df['efgpct'] = (df.team_fgm + (0.5 * df.team_fgm3)) / df.team_fga
    # effective field goal percentage defense
    df['efgpct_d'] = (df.opp_fgm + (0.5 * df.opp_fgm3)) / df.opp_fga
    # true shooting percentage
    df['trshpct'] = df.team_score / (2 * (df.team_fga + (0.44 * df.team_fta)))
    # true shooting defense
    df['trshpct_d'] = df.opp_score / (2 * (df.opp_fga + (0.44 * df.opp_fta)))
    # 3-point attempt percentage
    df['fga3pct'] = df.team_fga3 / df.team_fga
    # 3-point scoring percentage
    df['fg3pt_pct'] = (df.team_fgm3 * 3) / df.team_score
    # free throw scoring percentage
    df['ftpt_pct'] = df.team_ftm / df.team_score
    # 3-point + free throw scoring percentage
    df['fg3ftpt_pct'] = ((df.team_fgm3 * 3) + df.team_ftm ) / df.team_score
    # defensive 3-point attempt percentage
    df['fga3pct_d'] = df.opp_fga3 / df.opp_fga
    # field goal percentage
    df['fgpct'] = df.team_fgm / df.team_fga
    # defensive field goal percentage
    df['fgpct_d'] = df.opp_fgm / df.opp_fga
    # field goal percentage ratio
    df['fgpctrat'] = (df.fgpct + 1) / (df.fgpct_d + 1)
    # 3 point field goal percentage
    df['fg3pct'] = df.team_fgm3 / df.team_fga3
    # defensive 3-point percentage
    df['fg3pct_d'] = df.opp_fgm3 / df.opp_fga3
    # 3-point field goal attempt ratio
    df['fg3pctrat'] = (df.fg3pct + 1) / (df.fg3pct_d + 1)
    # assist to turnover ratio
    df['asttorat'] = (df.team_ast + 1) / (df.team_to + 1) - 1
    # defensive rebound percentage
    df['drbpct'] = df.team_dr / (df.team_dr + df.opp_or)
    # offensive rebound percentage
    df['orbpct'] = df.team_or / (df.team_or + df.opp_dr)
    # offensive rebound + turnover margin
    df['orbtomarg'] = (df.team_or - df.team_to) - (df.opp_or - df.opp_to)
    # orb + turnover margin / possessions
    df['orbtomarg_adpos'] = df['orbtomarg'] / df['pos']
    # rebound + turnover margin
    df['rbtomarg'] = (((df.team_or + df.team_dr) - df.team_to) -
                          ((df.opp_or + df.opp_dr) - df.opp_to))
    # rebounding + turnover margin / possessions
    df['rbtomarg_adpos'] = df['rbtomarg'] / df['pos']
    # rebounding margin
    df['rbmarg'] = (df.team_or + df.team_dr) - (df.opp_or + df.opp_dr)
    # rebounding margin / possessions
    df['rbmarg_adpos'] = df['rbmarg'] / df['pos']
    # turnover margin
    df['tomarg'] = df.opp_to - df.team_to
    # turnover margin pct
    df['tomarg_adpos'] = df.tomarg / df.pos
    # turnover percentage
    df['topct'] = df.team_to / df.pos
    # defensive turnover percentage
    df['topct_d'] = df.opp_to / df.pos
    # assist percentage
    df['astpct'] = df.team_ast / df.team_fgm
    # defensive assist percentage
    df['astpct_d'] = df.opp_ast / df.opp_fgm
    # non-forced turnovers
    df['tonf'] = df.team_to - df.opp_stl
    # non-forced turnovers / possessions
    df['tonf_adpos'] = df.tonf / df.pos
    # team foul rate
    df['foul_adpos'] = df.team_pf / df.pos
    # opponent foul rate
    df['foul_opp_adpos'] = df.opp_pf / df.pos
    # team defensive block rate
    df['blk_rate'] = df.team_blk / df.opp_fga
    # team defensive steal rate
    df['stl_rate'] = df.team_stl / df.pos
    # opponent steal rate
    df['stl_rate_opp'] = df.opp_stl / df.pos
    
    return df


"""
for d in days:
# create a groupby object for teams and seasons
df.sort_values(['t1_teamid', 'season', 'daynum'], inplace=True)
tsgroup = df.sort_values('daynum').groupby(['t1_teamid', 'season'])

# list of columns for sum aggregation
cols_to_sum = ['t1_win', 't1_loss']
tsgroup_sum = tsgroup[cols_to_sum].sum()
tsgroup_sum.columns = ['wins', 'losses']

# make list of columns for mean aggregation
aggmean = ['pos', 't1_score', 't2_score', 'scrmarg', 'scrmargpct', 'scrrat',
           'ftpct', 'ftrat', 'ftmpos', 'ftrat_d', 'efgpct', 'trshpct',
           'fg3pct', 'fg3pct_d', 'fga3pct', 'fga3pct_d', 'fg3pctrat', 'fgpct',
           'fgpct_d', 'fgpctrat', 'asttorat', 'drbpct', 'orbpct', 'orbtomarg',
           'rbtomarg', 'rbmarg', 'tomarg', 'topct', 'topct_d', 'astpct']

tsgroup_mean = tsgroup[aggmean].mean()
tsgroup_mean = tsgroup_mean.round(4)
tsgroup_mean = tsgroup_mean.rename(columns={'t1_score': 'ppg',
                                            't2_score': 'ppg_d'})

# scoring margin standard deviation aggregation`
tsgroup_std = tsgroup['scrmarg'].std()
tsgroup_std.rename('scrmargsd', inplace=True)
tsgroup_std = tsgroup_std.round(2)
tsgroup_mean['scrmargsd'] = tsgroup_std



def last5(group):
    #Count the number of wins in last 5 games.
    wins = group.iloc[-5:].sum()
    return wins

def last10(group):
    #Count the number of wins in last 10 games.
    wins = group.iloc[-10:].sum()
    return wins

def streak(y):
    #Compute number of consecutive events.
    streak = y * (y.groupby((y != y.shift()).cumsum()).cumcount() + 1)
    return int(streak[-1:])

def pctlast10(group):
    #Compute win percentage in last 10 games.
    wins = group.iloc[-10:].sum()
    return wins / 10.0

recent_list = []

print 'summing wins last 5'
recent_list.append(tsgroup['t1_win'].apply(last10).rename('wlast5'))
print 'summing wins last 10'
recent_list.append(tsgroup['t1_win'].apply(last10).rename('wlast10'))
print 'computing winning streak'
recent_list.append(tsgroup['t1_win'].apply(streak).rename('wstreak'))
print 'computing win pct last 10'
recent_list.append(tsgroup['t1_win'].apply(pctlast10).rename('wpctlast10'))
print 'computing losing streak'
recent_list.append(tsgroup['t1_loss'].apply(streak).rename('lstreak'))

tsgroup_rec = pd.concat(recent_list, axis=1)

mrg1 = pd.merge(tsgroup_sum, tsgroup_rec, how='outer',
                left_index=True, right_index=True)
mrg2 = pd.merge(mrg1, tsgroup_mean, how='outer',
                left_index=True, right_index=True)
mrg2 = mrg2.reset_index()

mrg2['winpct'] = mrg2.wins / (mrg2.wins + mrg2.losses)

mrg2 = mrg2.rename(columns={'t1_teamid': 'team_id'})
"""
