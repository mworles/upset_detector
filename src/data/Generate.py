"""Get yearly team tourney outcomes.

This script uses data on prior year tournament results to create a dataset of
the number of tournament games and wins for each team for each year. This data
is saved for use by other scripts to compute various team features. 

This script requires `pandas`. It imports the custom Clean module.

"""

import pandas as pd
import numpy as np
import Clean

def tourney_outcomes(datdir):
    """Function to count team games and wins for each tournament year.""" 
    # read in prior tournament results
    data_in = datdir + 'scrub/'
    tgames = pd.read_csv(data_in + 'ncaa_results.csv')

    # data is one row per game
    # separate winners and losers, to create a team-specific win indicator
    # winners
    wteams = tgames[['season', 'wteam']]
    wteams = wteams.rename(columns={'wteam': 'team_id'})
    wteams['win'] = 1
    
    # losers
    lteams = tgames[['season', 'lteam']]
    lteams = lteams.rename(columns={'lteam': 'team_id'})
    lteams['win'] = 0

    # combine data to create one row per team per game
    df = pd.concat([wteams, lteams], ignore_index=True)

    # columns to group by
    gcols = ['season', 'team_id']

    # count and sum number of rows per "group"
    df = df.groupby(gcols)['win'].aggregate(['count', 'sum']).reset_index()

    # count is the number of games, sum is the number of wins
    df = df.rename(columns={'count': 'games', 'sum': 'wins'})
    
    # write file
    Clean.write_file(df, datdir + 'interim/', 'tourney_outcomes')

def set_games(datdir):
    """Create data containing features for both teams in matchup."""
    r = pd.read_csv(datdir + 'scrub/ncaa_results.csv')
    s = sk = pd.read_csv(datdir + 'scrub/seasons.csv')
    df = pd.merge(r, s, on='season', how='inner')
    
    # add string date column to games
    df['date_id'] = df.apply(Clean.game_date, axis=1)
    
    # create outcome-neutral team identifiers
    team_cols = ['wteam', 'lteam']
    df = Clean.convert_team_id(df, team_cols, drop=True)
    
    # keep only necessary info to add features
    keep = ['season', 't1_team_id', 't2_team_id', 'date_id']
    df = df[keep]
    
    return df
    
def add_features(datdir, df):
    # import features data
    file_feat = datdir + 'features/team_features.csv'
    feat = pd.read_csv(file_feat)
    
    # cols to exclude when renaming features
    exc = ['team_id', 'season']
    # feature columns to rename
    tcols = [x for x in list(feat.columns) if x not in exc]
    
    t1dict = {}
    t2dict = {}
    
    for t in tcols:
        t1dict[t] = 't1_' + t
        t2dict[t] = 't2_' + t
    
    
    # create data for both teams with t1 and t2 prefix
    t1 = feat.copy().rename(columns=t1dict)
    t2 = feat.copy().rename(columns=t2dict)
    

    # merge for team 1
    merge_on=['season', 'team_id']
    df1 = pd.merge(df, t1, left_on=['season', 't1_team_id'], right_on=merge_on,
                   how='inner')
    df1 = df1.drop('team_id', axis=1)
    
    df2 = pd.merge(df1, t2, left_on=['season', 't2_team_id'], right_on=merge_on,
                   how='inner')
    df2 = df2.drop('team_id', axis=1)
    
    #df2 = Clean.set_gameid_index(df2)
    return df2

def make_matchups(datdir):
    matchups = set_games(datdir)
    df = add_features(datdir, matchups)
    
    # set unique game index
    df = Clean.set_gameid_index(df)
        
    # save data
    Clean.write_file(df, datdir + 'processed/', 'matchups', keep_index=True)

def get_upsets(datdir, df):
    
    def label_upset(row):
        
        if row['seed_abs'] >= 4:
            if row['t1_seed_dif'] > 0:
                if row['t1_marg'] > 0:
                    upset = 1
                else:
                    upset = 0
            else:
                if row['t1_marg'] > 0:
                    upset = 0
                else:
                    upset = 1
        else:
            upset = np.nan
        
        return upset

    # seeds data
    s = pd.read_csv(datdir + '/processed/matchups.csv', index_col=0)
    s = s[['t1_seed', 't2_seed']]
    # absolute seed difference
    s['t1_seed_dif'] = s['t1_seed'] - s['t2_seed']
    s['seed_abs'] = s['t1_seed_dif'].apply(abs)
    # merge with scores
    mrg = pd.merge(df, s, left_index=True, right_index=True)
    
    mrg['upset'] = mrg.apply(label_upset, axis=1)
    
    return mrg

def score_targets(datdir, df):
    # binary indicator of 1 if t1_team won
    df['t1_win'] = np.where(df['t1_score'] > df['t2_score'], 1, 0)

    # score margin 
    df['t1_marg'] = df['t1_score'] - df['t2_score']
    
    # upset labels
    df = get_upsets(datdir, df)
    
    # keep target columns
    df = df.loc[:, ['t1_win', 't1_marg', 'upset']]
    
    return df
    
def make_targets(datdir):
    file = datdir + '/scrub/ncaa_results.csv'
    df = pd.read_csv(file)
    
    # created ordered, outcome_neutral team identifier
    df = Clean.convert_team_id(df, ['wteam', 'lteam'], drop=False)
    # create unique game identifier and set as index
    df = Clean.set_gameid_index(df)
    # add column indicating score for each team
    scores = Clean.team_scores(df)
    df = score_targets(datdir, scores)
    
    Clean.write_file(df, datdir + '/processed/', 'targets', keep_index=True)
