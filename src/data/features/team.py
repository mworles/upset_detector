from src.data.transfer import DBAssist
from src.data import match
from src.data import clean
from src.data import generate
import pandas as pd
import numpy as np
import datetime

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
    # 2-point scoring percentage
    df['fg2pt_pct'] = ((df.team_fgm - df.team_fgm3) * 2) / df.team_score
    # 3-point + free throw scoring percentage
    df['fg3ftpt_pct'] = ((df.team_fgm3 * 3) + df.team_ftm ) / df.team_score
    # defensive 3-point scoring percentage
    df['fg3pt_pct'] = (df.opp_fgm3 * 3) / df.opp_score
    # defensive free throw scoring percentage
    df['ftpt_pct'] = df.opp_ftm / df.opp_score
    # defensive 2-point scoring percentage
    df['fg2pt_pct'] = ((df.opp_fgm - df.opp_fgm3) * 2) / df.opp_score
    # defensive 3-point + free throw scoring percentage
    df['fg3ftpt_pct'] = ((df.opp_fgm3 * 3) + df.opp_ftm ) / df.opp_score
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


def split_teams(df):
    df['winner'] = np.where(df['home_PTS'] > df['away_PTS'], 'H', 'A')
    keep_cols = ['gid', 'date', 'winner']
    home_cols = keep_cols + [x for x in df.columns if 'home_' in x]
    away_cols = keep_cols + [x for x in df.columns if 'away_' in x]
    
    home = df[home_cols].copy()
    away = df[away_cols].copy()
    
    home.columns = [x.replace('home_', '') for x in home.columns]
    away.columns = [x.replace('away_', '') for x in home.columns]
    
    home['loc'] = 'H'
    away['loc'] = 'A'
    
    all = pd.concat([home, away], sort=False)
    
    return all

def clean_box(df):
    col_map = {'OFF': 'or',
               'DEF': 'dr',
               'A': 'ast',
               'PF': 'pf',
               'STL': 'stl',
               'BLK': 'blk',
               'TO': 'to',
               'PTS': 'score'}

    fg = map(lambda x: x.split('-'), df['FGMA'].values)
    fg3 = map(lambda x: x.split('-'), df['3PMA'].values)
    ft = map(lambda x: x.split('-'), df['FTMA'].values)
    
    df['fgm'] = [x[0] for x in fg]
    df['fga'] = [x[1] for x in fg]
    df['fgm3'] = [x[0] for x in fg3]
    df['fga3'] = [x[1] for x in fg3]
    df['ftm'] = [x[0] for x in ft]
    df['fta'] = [x[1] for x in ft]
    df['numot'] = (df['MIN'].astype(int) - 200) / 25
    
    df = df.rename(columns=col_map)
    winner = (df['winner'] == df['loc'])

    df = df.drop(columns=['FGMA', '3PMA', 'FTMA', 'MIN', 'TOT', 'winner', 'loc'])
    
    dfw = df[winner]
    dfl = df[~winner]
    
    change_cols = list(df.columns)
    not_change = ['gid', 'date', 'team', 'numot']
    change_cols = [x for x in change_cols if x not in not_change]
    win_cols = ['w' + x for x in change_cols]
    win_map= {k:v for k,v in zip(change_cols, win_cols)}
    dfw = dfw.rename(columns=win_map)
    dfw = dfw.rename(columns={'team': 'wteam'})
    
    lose_cols = ['l' + x for x in change_cols]
    lose_map = {k:v for k,v in zip(change_cols, lose_cols)}
    dfl = dfl.rename(columns=lose_map)
    #dfl = match.id_from_name(dfl, 'team_tcp', 'team')
    dfl = dfl.rename(columns={'team': 'lteam'})
    dfl = dfl.drop(columns=['numot'])
    
    merge_on = ['gid', 'date']
    mrg = pd.merge(dfw, dfl, how='inner', left_on=merge_on, right_on=merge_on)
    
    mrg['season'] = map(clean.season_from_date, mrg['date'].values)
    
    # add dayzero date to create daynum
    seasons = DBAssist().return_data('seasons', modifier="WHERE season = 2020")
    seasons = seasons[['season', 'dayzero']]
    
    mrg['dayzero'] = seasons['dayzero'].values[0]
    make_dt = lambda x: datetime.datetime.strptime(x, "%m/%d/%Y")
    mrg['dayzero'] = mrg['dayzero'].apply(make_dt)
    make_dt = lambda x: datetime.datetime.strptime(x, "%Y/%m/%d")
    mrg['date_dt'] = mrg['date'].apply(make_dt)
    mrg['daynum'] = (mrg['date_dt'] - mrg['dayzero']).apply(lambda x: x.days)
    
    mrg = mrg.drop(columns=['dayzero', 'date_dt', 'gid', 'date'])
    
    return mrg

def box_stats_by_team(mod=None):
    df = DBAssist().return_data('game_box', modifier=mod)
    st = split_teams(df)
    cb = clean_box(st)
    sbt = generate.games_by_team(cb)
    sbt = sbt.rename(columns={'team_id': 'team'})
    sbt = match.id_from_name(sbt, 'team_tcp', 'team')
    sbt = sbt.rename(columns={'opp_id': 'opp'})
    sbt = match.id_from_name(sbt, 'team_tcp', 'opp', how='left')
    return sbt

def sum_stats(gb):
    cols_to_sum = ['team_win', 'team_loss']
    tgb_sum = gb[cols_to_sum].sum()
    tgb_sum.columns = ['wins', 'losses']
    tgb_sum = tgb_sum.reset_index()
    return tgb_sum

def mean_stats(df):
    gb = df.groupby('team_id')
    # make list of columns for mean aggregation
    aggmean = df.columns
    not_mean = ['team_win', 'team_loss', 'team_id', 'opp_id', 'date']
    aggmean = [x for x in aggmean if x not in not_mean]

    tgb = df.groupby('team_id')
    ts_mean = tgb[aggmean].mean()
    ts_mean = ts_mean.round(4)
    ts_mean = ts_mean.rename(columns={'team_score': 'ppg',
                                      'opp_score': 'ppg_d'})
    ts_mean = ts_mean.reset_index()
    return ts_mean

def pct_last(group, n=5):
    #Compute win percentage in last n games.
    wins = group.iloc[-n:].sum()
    return wins / float(n)

def streak(y):
    #Compute number of consecutive events.
    streak = y * (y.groupby((y != y.shift()).cumsum()).cumcount() + 1)
    return int(streak[-1:])

def prep_stats_by_team(df):
    start_cols = df.columns.tolist()
    
    # add computed stats to data
    df = compute_game_stats(df)
    
    # keep computed stats and select columns
    new_cols = [x for x in list(df.columns) if x not in start_cols]
    new_cols.remove('pos')
    keep_cols = ['season', 'daynum', 'team_id', 'team_score', 'opp_id',
                 'opp_score']
    keep_cols.extend(new_cols)
    df = df[keep_cols]
    
    # add date to each game
    dfs = DBAssist().return_data('seasons')
    dfs = dfs[['season', 'dayzero']]
    df = pd.merge(df, dfs, how='inner', left_on='season', right_on='season')
    df['date'] = df.apply(clean.game_date, axis=1)
    
    # remove cols not needed
    df = df.drop(['season', 'daynum', 'dayzero'], axis=1)
    
    return df


def compute_summaries(df, max_date=None):
    if max_date is not None:
        df = df[df['date'] < max_date]
    else:
        max_date = df['date'].max()
    
    df = df.sort_values(['team_id', 'date'])
    
    tgb_mean = mean_stats(df)
    
    gb = df.groupby('team_id')
    tgb_sum = sum_stats(gb)

    tgb_std = gb['scrmarg'].std()
    tgb_std = tgb_std.rename('scrmargsd').round(2)
    tgb_std = tgb_std.reset_index()
    
    recent_list = []
    
    recent_list.append(gb['team_win'].apply(streak).rename('wstreak'))
    recent_list.append(gb['team_loss'].apply(streak).rename('lstreak'))

    # need to remove teams with < 5/10 games
    # on merge below these teams will be assigned missing values
    game_count = gb['team_win'].count().reset_index()
    have_five = game_count[game_count['team_win'] >=5]['team_id'].values
    have_ten = game_count[game_count['team_win'] >=10]['team_id'].values
    df5 = df[df['team_id'].isin(have_five)]
    df10 = df[df['team_id'].isin(have_ten)]

    gb5 = df5.groupby('team_id')
    gb10 = df10.groupby('team_id')
    
    last_5 = lambda x: pct_last(x, n=5)
    pct_last_5 = gb5['team_win'].apply(last_5).rename('wpctlast5')
    
    last_10 = lambda x: pct_last(x, n=10)
    pct_last_10 = gb10['team_win'].apply(last_10).rename('wpctlast10')

    tgb_rec = pd.merge(pct_last_5, pct_last_10, left_on='team_id',
                      right_on='team_id', how='outer')

    mrg1 = pd.merge(tgb_sum, tgb_mean, how='outer',
                    left_on='team_id', right_on='team_id')
    mrg2 = pd.merge(mrg1, tgb_std, how='outer', left_on='team_id',
                    right_on='team_id')
    mrg3 = pd.merge(mrg2, tgb_rec, how='outer',
                    left_on='team_id', right_on='team_id')

    mrg3['winpct'] = mrg3.wins / (mrg3.wins + mrg3.losses)
    
    mrg3['date'] = max_date
    mrg3['season'] = clean.season_from_date(max_date)
    
    return mrg3
