from src.data import Transfer
from src.data import Match
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
    
    dfw = Match.id_from_name(df, 'team_tcp', 'team', drop=False)
    dfw = dfw.rename(columns={'team_id': 'wteam'})
    
    lose_cols = ['l' + x for x in change_cols]
    lose_map = {k:v for k,v in zip(change_cols, lose_cols)}
    dfl = dfl.rename(columns=lose_map)
    dfl = Match.id_from_name(df, 'team_tcp', 'team', drop=False)
    dfl = dfw.rename(columns={'team_id': 'lteam'})

    return dfw

def convert(df):
    home_won = df['home_score'] > df['away_score']
    
    col_map = {'FGMA'}
    
    df['wteam'] = np.where(home_won, df['home_team_id'], df['away_team_id'])
    df['lteam'] = np.where(home_won, df['away_team_id'], df['home_team_id'])
    df['wscore'] = np.where(home_won, df['home_score'], df['away_score'])
    df['lscore'] = np.where(home_won, df['away_score'], df['home_score'])
    df['wloc'] = np.where(home_won, 'H', 'A')
    df['wloc'] = np.where(df['neutral'] == 1, 'N', df['wloc'])
    
    return df
