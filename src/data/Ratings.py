import pandas as pd
import numpy as np
import os
import Clean, Generate, Transfer, Match
import math
import multiprocessing as mp

def reduce_margin(df, cap):
    # reduce outlier scores to limit impact of huge blowouts
    absdif = (df['team_score'] - df['opp_score']).abs()
    over15 = (absdif > cap).values
    marg15 = (absdif - cap).values
    team_won = (df['team_score'] > df['opp_score']).values
    team_score = df['team_score'].values
    opp_score = df['opp_score'].values

    reduce_mat = zip(over15, team_won, marg15, team_score, opp_score)

    def reduced_scores(x):
        
        team_score = x[-2]
        opp_score = x[-1]
        
        # if margin was over 15
        if x[0] == True:
            # if team won
            if x[1] == True:
                # reduce score by margin over 15
                team_score -= x[2]
            else:
                opp_score -= x[2]
        
        return [team_score, opp_score]

    team_opp = map(lambda x: reduced_scores(x), reduce_mat)

    df['team_score'] = [x[0] for x in team_opp]
    df['opp_score'] = [x[1] for x in team_opp]
    
    return df


def add_weights(df, date_col = 'date', wc = 1):
    """wc is a weight coeffficient where values above 1 apply a stronger
    adjustment for recency"""
    group_by_cols = ['team_team_id']
    # add game weights
    df = df.sort_values(group_by_cols + [date_col], ascending=True)
    df['game'] = 1
    df['game_n'] = df.groupby(group_by_cols)['game'].transform('cumsum')
    df['game_max'] = df.groupby(group_by_cols)['game_n'].transform('max')
    
    # weight coefficient
    rel_rec = 1- (((df['game_max']*wc - df['game_n']*wc) / df['game_max']*wc))
    df['weight'] = 1 / (1 + (0.5**(5*rel_rec)) * (math.e**-rel_rec))
    
    df['weight'] = df['weight'].round(4)
    df = df.drop(['game', 'game_n', 'game_max'], axis=1)
    
    return df


# adjust scores for team location
def location_adjustment(df):
    """Perform locationn adjustment to scores for team ratings."""
    adjust_dict = {'A': 1.875, 'H': -1.875, 'N':0}
    
    t1_adjust = map(lambda x: adjust_dict[x], df['t1_loc'].values)
    t2_adjust = map(lambda x: adjust_dict[x], df['t2_loc'].values)
    
    df['t1_score'] = df['t1_score'] + t1_adjust
    df['t2_score'] = df['t2_score'] + t2_adjust
    
    return df

def compute_posessions(df):
    """Estimate the number of possessions for each team in the game."""
    # estimate possessions per team
    pos = ((df['wfga'] + df['lfga']) + 0.475 * (df['wfta'] + df['lfta']) - 
           (df['wor'] + df['lor']) + (df['wto'] + df['lto'])) / 2
    df['pos'] = pos.round(2)
    return df

def games_by_team(df):
    """Reshape games data so that each team has own row for each game.""" 
    t1 = df.copy()
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
    df = pd.concat([t1, t2], sort=True)
    
    cols_rename = [x.replace('t1_', 'team_') for x in df.columns]
    cols_rename = [x.replace('t2_', 'opp_') for x in cols_rename]
    
    df.columns = cols_rename

    return df

def games_ratings(datdir):
    """Create a dataset with the requirements for computing team ratings."""
    df = Generate.neutral_games(datdir)
    df['date'] = df['date_id'].str.replace('_', '/')
    df = Generate.team_locations(df)
    df = location_adjustment(df)
    df = compute_posessions(df)
    keep = ['season', 'date', 't1_team_id', 't2_team_id', 't1_score', 't2_score',
            'pos']
    df = df[keep]
    df = games_by_team(df)
    df = reduce_margin(df, cap=22)

    # compute points per 100 possessions
    df['team_off'] = (100 * (df['team_score'] / df['pos'])).round(3)
    df['team_def'] = (100 * (df['opp_score'] / df['pos'])).round(3)
    
    return df

def game_box_convert(df):
    home_won = df['home_score'] > df['away_score']
    df['wteam'] = np.where(home_won, df['home_team_id'], df['away_team_id'])
    df['lteam'] = np.where(home_won, df['away_team_id'], df['home_team_id'])
    df['wscore'] = np.where(home_won, df['home_score'], df['away_score'])
    df['lscore'] = np.where(home_won, df['away_score'], df['home_score'])
    df['wloc'] = np.where(home_won, 'H', 'A')
    df['wloc'] = np.where(df['neutral'] == 1, 'N', df['wloc'])
    return df

def game_box_games(df):
    # add season column
    season = max([x.split('/')[0] for x in df['date'].values])
    df['season'] = season
    
    # remove games without scores, so below computations complete
    df = df.dropna(how='any', subset=['home_score', 'away_score', 'neutral'])
    
    # add numeric team id
    df = Match.id_from_name(df, 'team_tcp', 'away_team', drop=False)
    df = Match.id_from_name(df, 'team_tcp', 'home_team', drop=False)
    
    # convert columns to apply neutral id function
    df = game_box_convert(df)
    # create team_1 and team_2 id identifer columns
    df = Generate.convert_team_id(df, ['wteam', 'lteam'], drop=False)
    # add column indicating scores and locations for each team
    df = Generate.team_scores(df)
    df = Generate.team_locations(df)
    # adjust score for location of game
    df = location_adjustment(df)
    
    return df

def game_box_possessions(df):
    
    def split_makes_attempts(elem):
        try:
            elem_split = elem.split('-')
            return (elem_split[0], elem_split[1])
        except:
            return None

    for team in ['home_', 'away_']:
        old_col = team + 'FGMA'
        makes_attempts = map(split_makes_attempts, df[old_col].values)
        new_col = team + 'fga'
        df[new_col] = [x[1] if x is not None else None for x in makes_attempts]
        old_col = team + 'FTMA'
        makes_attempts = map(split_makes_attempts, df[old_col].values)
        new_col = team + 'fta'
        df[new_col] = [x[1] if x is not None else None for x in makes_attempts]

    # remove missing rows missing any posession cols
    pos_list = ['_fta', '_fga', '_OFF', '_TO']
    pos_cols = [x for x in df.columns if any(ele in x for ele in pos_list)]
    pos_cols = [x for x in pos_cols if not '_TOT' in x]

    df = df.dropna(subset=pos_cols, how='any')
    bp = df[['gid'] + pos_cols]
    bp = bp.set_index('gid').astype(int)

    # compute posessions
    pos = ((bp['home_fga'] + bp['away_fga']) +          # combined fg attempts
           0.475 * (bp['home_fta'] + bp['away_fta'])    # estimate ft adjustment 
           - (bp['home_OFF'] + bp['away_OFF']) +        # less offensive rbs
           (bp['home_TO'] + bp['away_TO'])) / 2         # combined TOs

    bp['pos'] = pos.round(2)
    bp = bp.reset_index().drop(columns=pos_cols)
    bp = bp[bp['pos'] != 0]
    return bp

def game_box_for_ratings(date=None):
    dba = Transfer.DBAssist()
    dba.connect()
    table = dba.return_table('game_scores')
    tbox = dba.return_table('game_box')

    dfo = pd.DataFrame(table[1:], columns=table[0])
    box = pd.DataFrame(tbox[1:], columns=tbox[0])
    
    # if date given, only include games on that date
    if date is not None:
        dfo = dfo[dfo['date'] == date]
        box = box[box['date'] == date]
    
    df = game_box_games(dfo)
    pos = game_box_possessions(box)
    
    # join possessions to scores
    df = pd.merge(df, pos, left_on='gid', right_on='gid', how='inner')

    # assign unique game identifer from date and teams
    df = Generate.set_gameid_index(df, date_col='date', full_date=True,
                                   drop_date=False)

    # create duplicate of games so each team has row for each game
    df = games_by_team(df)
    # adjust victory margins to minimize impact of blowouts
    df = reduce_margin(df, cap=22)

    # compute points scored and allowed per 100 possessions
    # pace-independent metric of efficiency
    df['team_off'] = (100 * (df['team_score'] / df['pos'])).round(3)
    df['team_def'] = (100 * (df['opp_score'] / df['pos'])).round(3)

    # keep only columns needed for ratings program
    keep = ['date', 'pos', 'season', 'team_score', 'team_team_id', 'opp_score',
            'opp_team_id', 'team_off', 'team_def']
    df = df[keep]
    
    df = df.sort_values('date')
    
    return df


def create_team_dict(df, col_name, group_by='opp'):
    team_dict = {}
    
    if group_by == 'team':
        dict_cols = ['opp_team_id'] + [col_name] + ['weight']
    else:
        dict_cols = ['team_team_id'] + [col_name] + ['weight']
    
    gb = df.groupby(group_by + '_team_id')
    
    for g, d in gb:
        team_dict[g] = d[dict_cols].values

    return team_dict

def opponent_mean(team_opp, team_dict, weighted=False):
    opponents_scores = [x[1] for x in team_dict[team_opp[1]] if x[0] != team_opp[0]]
    # if opponents have no other opponents in data
    if len(opponents_scores) < 1:
        opponents_scores = [x[1] for x in team_dict[team_opp[1]]]

    if not weighted:
        opponents_mean = np.mean(opponents_scores)
    else:
        weights = [x[2] for x in team_dict[team_opp[1]] if x[0] != team_opp[0]]
        opponents_mean = np.average(opponents_scores, weights=weights)
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

def adjust_tempo(df, id_array, input='adjusted', weighted=True):
    adjust_col = 'pos'
    if input == 'adjusted':
        adjust_col += '_adj'
    
    team_dict = create_team_dict(df, adjust_col, group_by='team')
    df['opp_pos_mn'] = map(lambda x: opponent_mean(x, team_dict, weighted), id_array)
    df['team_pos_mn'] = df.groupby('team_team_id')[adjust_col].transform('mean')
    team_pos_diff = df['team_pos_mn'] - df['pos_gm']
    opp_pos_diff = df['opp_pos_mn'] - df['pos_gm']
    exp_diff = df['pos'] - (df['pos_gm'] + team_pos_diff + opp_pos_diff)
    pos_adj_pct = exp_diff / (df['team_pos_mn'] + df['opp_pos_mn'])
    df['pos_adj'] = df['team_pos_mn'] + (df['team_pos_mn'] * pos_adj_pct)
    df = df.drop(columns=['team_pos_mn', 'opp_pos_mn'])
    return df

def adjust_offense(df, id_array, input='adjusted', weighted=True):
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
    # values 2d matrix with opponents' opponent id and points dfowed
    team_dict = create_team_dict(df, adjust1_col, group_by=adjust1_grp)
    # mean points dfowed by opponents in games with other teams
    df[adjust1_dem] = map(lambda x: opponent_mean(x, team_dict, weighted), id_array)
    # adjust offense to grand mean and defense
    df['team_off_adj'] = (df['team_off'] * df['team_off_gm']) / df[adjust1_dem]
    
    return df

def adjust_defense(df, id_array, weighted=True):
    # keys are team ids
    # values 2d matrix with opponent id and points scored
    team_dict = create_team_dict(df, 'team_off_adj', group_by='team')
    # mean points scored by opponent in games with other teams
    df['opp_off_adj'] = map(lambda x: opponent_mean(x, team_dict, weighted), id_array)
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
    df = adjust_tempo(df, id_array, input=input, weighted=False)

    return df

def schedule_strength(df_games, df_teams):
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
    sos = schedule_strength(df, df_teams)
    df_teams = pd.merge(df_teams, sos, on=['team_id'])
    # round all ratings
    df_teams = df_teams.round(decimals=3)
    return df_teams

def get_ratings(data, n_iters=10):
    dfrun = data.copy()

    n_iters = n_iters
    n = 0

    while n < n_iters:

        input = 'raw'
        if n >= 1:
            input = 'adjusted'
        
        dfrun = adjusted_scores(dfrun, input=input)

        n += 1

    return condense_df(dfrun)

def minimum_day(df, n_games=3):
    df = df.sort_values(['team_team_id', 'daynum'], ascending=True)
    df['game'] = 1 
    df['game_n'] = df.groupby(['team_team_id'])['game'].transform('cumsum')
    day_min = df[df['game_n'] == n_games]['daynum'].max()
    return day_min

def day_number(df):
    udates = pd.unique(df.sort_values('date')['date']).tolist()
    date_daynum = {k:v for (k, v) in zip(udates, range(0, len(udates) + 1))}
    df.loc[:, 'daynum'] = df['date'].map(date_daynum)
    return df

def run_day(df, day_max = None, n_iters=15):
    cols_numeric = [c for c in df.columns if c not in ['date']]
    for c in cols_numeric:
        df[c] = df[c].astype('float')

    df = day_number(df)

    year = max(df['season'].values)
    df = df[df['season'] == year]
    if day_max is None:
        day_max = max(df['daynum'].values)
    df = df[df['daynum'] < day_max]
    date = max(df['date'])
    print 'season %s, day %s' % (year, day_max)
    df = add_weights(df, date_col = 'date', wc = 0.75)
    df_teams = get_ratings(df, n_iters=n_iters)
    df_teams['season'] = year
    df_teams['date'] = date
    #return df_teams
    rows = Transfer.dataframe_rows(df_teams)
    Transfer.insert('ratings_at_day', rows)


def run_year(df, n_iters = 15, multiprocessing=True):
    df = day_number(df)
    year = pd.unique(df['season'])[0]
    day_min = minimum_day(df, n_games=3)
    all_days = list(set(df['daynum'].values))
    all_days.sort()
    rate_days = [x for x in all_days if x >=day_min]
    
    if multiprocessing==False:
        results = map(lambda x: run_day(df, x, n_iters=n_iters), rate_days)
    
    else:
        output = mp.Queue()
        processes = [mp.Process(target=run_day, args=(dfy, x, n_iters=n_iters)) for x in rate_days]
        for p in processes:
            p.start()
        results = [output.get() for p in processes]


def run_years(n_iters=15, multiprocessing=True):
    dba = Transfer.DBAssist()
    dba.connect()
    df = dba.return_df('games_for_ratings')
    years = list(set(df['season']))
    years.sort()

    for year in years:
        year = float(year)
        dfy = df[df['season'] == year].copy()
        run_year(df, n_iters=n_iters, multiprocessing=multiprocessing)
