"""Interim data generation.

This module contains functions used to generate interim data. Each function
will:
1) use raw data to create an intermediate dataset used to compute features
2) add additional data to an object, such as a dataframe.
3) modify data structure for inputting to other operations

This script requires `pandas` and `numpy`. It imports the custom Clean module.

"""

import pandas as pd
import numpy as np
import Clean, Match, Ratings

def set_gameid_index(df, date_col='date_id', full_date=False, drop_date=True):
    """
    Returns dataframe with new index in date_team1_team2 format. 
    Ensures a unique identifier for each game to use as primary key. 

    Arguments
    ----------
    df: pandas dataframe
        Data to set the index on. Must have 't1_team_id', 't2_team_id', and 
        either 'date_id' or 'season' as columns, depending on whether the full
        date is used to create the index. 
    full_date: boolean
        If true, use the full date (year_mon_day) in index. If false, year only.
    drop_date: boolean
        If true, remove the date column before returning. If false, remove it. 
    
    """
    # identify date series as either full date or season
    if full_date == True:
        date = df[date_col].str.replace('/', '_')
        # remove date col if indicated
        if drop_date == True:
            df = df.drop([date_col], axis=1)
    else:
        date = df['season'].apply(str)
    
    # need team numeric identifiers as string type
    id_lower = df['t1_team_id'].astype(int).astype(str)
    id_upper = df['t2_team_id'].astype(int).astype(str)
    
    # game_id is date combined with both team series
    df['game_id'] = date + '_' + id_lower + '_' + id_upper
    df = df.set_index('game_id')
    
    # return data with new index
    return df

def tourney_outcomes(datdir):
    """Uses game results to create team performance indicators for each 
    tournament year.""" 
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
    
    # add 1 to season so indicators represent past performance
    df['season'] = df['season'] + 1
    
    # write file
    Clean.write_file(df, datdir + 'interim/', 'tourney_outcomes')

def convert_team_id(df, id_cols, drop=True):
    """Return data with neutral team identifiers 't1_team_id' and 't2_team_id' 
    where 't1_team_id' is the numerically-lower id.
    In the raw data, the identifers are separated into game winners and losers.
    This function creates outcome-neutral idenfiers to prevent leakage.
    
    Arguments
    ----------
    df: pandas dataframe
        A dataframe containing two team identifer columns.
    id_cols: list
        The list of length 2 with the names of the team identifer columns.
    drop: boolean
        If true, remove the original team identifer columns before returning 
        data.
    
    """
    # use min and max to create new identifiers
    df['t1_team_id'] = df[id_cols].min(axis=1).astype(int)
    df['t2_team_id'] = df[id_cols].max(axis=1).astype(int)
    # drop original identifers if desired
    if drop == True:
        df = df.drop(columns=id_cols)
    
    return df

def set_games(datdir):
    """Establish neutral team ids and date for each game."""
    r = pd.read_csv(datdir + 'scrub/ncaa_results.csv')
    s = pd.read_csv(datdir + 'scrub/seasons.csv')
    df = pd.merge(r, s, on='season', how='inner')
    
    # add string date column to games
    df['date_id'] = df.apply(Clean.game_date, axis=1)
    
    # create outcome-neutral team identifiers
    team_cols = ['wteam', 'lteam']
    df = convert_team_id(df, team_cols, drop=True)
    
    # keep only necessary info to add features
    keep = ['season', 't1_team_id', 't2_team_id', 'date_id']
    df = df[keep]
    
    return df
    
def matchup_features(datdir, df):
    """Add features for both teams in a matchup.
    
    Arguments
    ----------
    datdir: string
        Project data directory. 
    df: pandas dataframe
        Requires columns 'season', 't1_team_id', 't2_team_id'.
    """
    def add_team_features(df_game, df_features, team_id):
        """Nested function to add features for one team."""
        merge_on=['season', 'team_id']
        df = pd.merge(df_game, df_features, left_on=['season', team_id],
                      right_on=merge_on, how='inner')
        df = df.drop('team_id', axis=1)
        return df
        
    # import features data
    file_feat = datdir + 'features/team_features.csv'
    feat = pd.read_csv(file_feat)
    
    # cols to exclude when renaming features
    exc = ['team_id', 'season']
    # get list of feature columns to rename
    tcols = [x for x in list(feat.columns) if x not in exc]
    
    # empty dicts for creating map to rename columns
    t1dict = {}
    t2dict = {}
    
    # create separate maps for t1 and t2 teams
    for t in tcols:
        t1dict[t] = 't1_' + t
        t2dict[t] = 't2_' + t
    
    # create data for both teams with t1 and t2 prefix
    t1 = feat.copy().rename(columns=t1dict)
    t2 = feat.copy().rename(columns=t2dict)
    
    # add features for both teams in matchup
    df = add_team_features(df, t1, 't1_team_id')
    df = add_team_features(df, t2, 't2_team_id')
    
    return df

def make_matchups(datdir):
    """Convenience function to set up games, add features, and save file."""
    # identifies outcome-neutral team identifers and game date
    matchups = set_games(datdir)
    
    # add features to both teams in matchup
    df = matchup_features(datdir, matchups)
    
    # set unique game index
    df = set_gameid_index(df, full_date=True, drop_date=False)
        
    # save data
    Clean.write_file(df, datdir + 'processed/', 'matchups', keep_index=True)


def team_scores(df):
    """Return data with neutral team scores 't1_score' and 't2_score' 
    where 't1_score' is the score for the team with numerically-lower id.
    In the raw data, game scores are separated into game winners and losers.
    This function creates outcome-neutral scores to prevent leakage.
    
    Arguments
    ----------
    df: pandas dataframe
        A dataframe containing game results. Requires 'wteam' for winning team
        id, 'wscore' for winning team score, 'lteam' for losing team id, and 
        'lscore' for losing team score, 't1_team_id' for one team and 
        't2_team_id' for otehr team. 

    """
    
    score_mat = df[['wteam', 'wscore', 'lteam', 'lscore']].values
    
    def team_score(row, team_id):
        if team_id == row[0]:
            return row[1]
        else:
            return row[3]
    
    t1 = df['t1_team_id'].values
    t2 = df['t2_team_id'].values
    
    df['t1_score'] = [team_score(x, y) for x, y in zip(score_mat, t1)]
    df['t2_score'] = [team_score(x, y) for x, y in zip(score_mat, t2)]
    
    return df


def get_upsets(datdir, df):
    """Apply upset label to games.
    
    Arguments
    ----------
    datdir: string
        Project data directory. 
    df: pandas dataframe
        Requires unique game id index and 't1_marg' column for team 1
        score margin.
    """
    
    def label_upset(row):
        """Nested function to apply to rows and identify upsets."""
        # absolute seed difference is at least 4
        if row['seed_abs'] >= 4:
            # if team 1 seed is higher
            if row['t1_seed_dif'] > 0:
                # if team 1 won game 
                if row['t1_marg'] > 0:
                    # is an upset
                    upset = 1
                else:
                    # team 1 lost and not an upset
                    upset = 0
            # else team 1 seed is lower
            else:
                # if team 1 won game
                if row['t1_marg'] > 0:
                    # not an upset
                    upset = 0
                else:
                    # team 1 lost game and is an upset
                    upset = 1
        # seed difference is less than 4 and upset label not applied
        else:
            upset = np.nan
        
        # return row
        return upset

    # to identify upsets need team seeds
    s = pd.read_csv(datdir + '/processed/matchups.csv', index_col=0)
    s = s[['t1_seed', 't2_seed']]
    
    # create temp columns needed to use label_upset function
    s['t1_seed_dif'] = s['t1_seed'] - s['t2_seed']
    s['seed_abs'] = s['t1_seed_dif'].apply(abs)
    
    # merge with scores
    mrg = pd.merge(df, s, left_index=True, right_index=True)
    
    # apply upset label using nested function above
    mrg['upset'] = mrg.apply(label_upset, axis=1)
    
    # remove the added columns
    mrg = mrg.drop(['t1_seed', 't2_seed', 't1_seed_dif', 'seed_abs'], axis=1)
    
    return mrg

def score_targets(datdir, df):
    """Get targets from team scores.
    
    Arguments
    ----------
    datdir: string
        Project data directory. 
    df: pandas dataframe
        Requires unique game id index and columns 't1_score' and 't2_score'.
    """
    # binary indicator of 1 if t1_team won
    df['t1_win'] = np.where(df['t1_score'] > df['t2_score'], 1, 0)

    # score margin as t1_team score minus t2_team score
    df['t1_marg'] = df['t1_score'] - df['t2_score']
    
    # add upset labels
    df = get_upsets(datdir, df)
    
    return df
    
def make_targets(datdir):
    """Create dataset of targets for prediction.
    
    Arguments
    ----------
    datdir: string
        Project data directory.
    """
    # read in data file with game results
    file = datdir + '/scrub/ncaa_results.csv'
    df = pd.read_csv(file)
    # import and merge seasons for dates
    s = pd.read_csv(datdir + 'scrub/seasons.csv')
    df = pd.merge(df, s, on='season', how='inner')

    # add string date column to games
    df['date_id'] = df.apply(Clean.game_date, axis=1)

    # create outcome-neutral team identifier
    df = convert_team_id(df, ['wteam', 'lteam'], drop=False)
    # create unique game identifier and set as index
    df = set_gameid_index(df, full_date=True, drop_date=False)

    # add column indicating score for each team
    scores = team_scores(df)
    # create targets data
    df = score_targets(datdir, scores)
    
    # spreads
    file_spreads = datdir + '/interim/spreads.csv'
    spreads = pd.read_csv(file_spreads, index_col=0)
    spreads = spreads.drop(['t1_team_id', 't2_team_id'], axis=1)
    
    # merge spreads with targets
    df = pd.merge(df, spreads, how='left', left_index=True, right_index=True)
    
    # keep target columns
    df = df.loc[:, ['t1_win', 't1_marg', 'upset', 't1_spread']]
    
    t1_smarg = - df['t1_spread']
    
    def cover_spread(x):
        margin = x[0]
        spread = x[1]
        if spread < 0:
            if margin >= -spread:
                result = 1
            else:
                result = 0
        elif spread > 0:
            if margin < 0:
                if -margin < spread:
                    result = 1
                else:
                    result = 0
            else:
                result = 1
        else:
            if margin >= 0:
                result = 1
            else:
                result = 0
        return result
    
    margins_spreads = df[['t1_marg', 't1_spread']].values
    df['t1_cover'] = map(cover_spread, margins_spreads)    
    
    # save data file
    Clean.write_file(df, datdir + '/processed/', 'targets', keep_index=True)


def get_location(row):
    "Returns string indicator of game location for team."
    lteam_dict = {'A': 'H', 'H': 'A', 'N': 'N'}
    if row[0] == row[2]:
        return row[1]
    else:
        return lteam_dict[row[1]]

def team_locations(df):
    "Given matrix of winner id, winner location, and team id, returns vector of game locations."
    # matrix for t1_teams
    t1_mat = df[['wteam', 'wloc', 't1_team_id']].values
    t2_mat = df[['wteam', 'wloc', 't2_team_id']].values
    
    df['t1_loc'] = map(lambda x: get_location(x), t1_mat)
    df['t2_loc'] = map(lambda x: get_location(x), t2_mat)
    
    return df

def neutral_games(datdir):
    """Create dataset of games with neutral team id, scores, and locations."""
    # read in data file with game results
    files = Clean.list_of_files(datdir + 'scrub/', tag = 'results_dtl')
    df_list = [pd.read_csv(x) for x in files]

    # combine df games to one dataset
    df = pd.concat(df_list, sort=False)

    # import and merge seasons for dates
    s = pd.read_csv(datdir + 'scrub/seasons.csv')
    df = pd.merge(df, s, on='season', how='inner')

    # add string date column to games
    df['date_id'] = df.apply(Clean.game_date, axis=1)

    # create outcome-neutral team identifier
    df = convert_team_id(df, ['wteam', 'lteam'], drop=False)
    # create unique game identifier and set as index
    df = set_gameid_index(df, full_date=True, drop_date=False)

    # add column indicating score for each team
    scores = team_scores(df)
    scores = scores.sort_index()
    
    return scores


def convert_past_games():
    df1 = Transfer.return_data('reg_results')
    df1['game_cat'] = 'regular'

    df2 = Transfer.return_data('nit_results')
    df2 = df2.rename(columns={'secondarytourney': 'game_cat'})

    df3 = Transfer.return_data('ncaa_results')
    df3['game_cat'] = 'ncaa'

    df = pd.concat([df1, df2, df3], sort=True)

    s = Transfer.return_data('seasons')
    s = s[['season', 'dayzero']]

    df = pd.merge(df, s, on='season', how='inner')
    # add string date column to games
    df['date'] = df.apply(Clean.game_date, axis=1)
    df['season'] = df['season'].astype(int)
    return df

def make_game_info(df):
    # create team_1 and team_2 id identifer columns
    df = convert_team_id(df, ['wteam', 'lteam'], drop=False)
    df = set_gameid_index(df, date_col='date', full_date=True,
                                   drop_date=False)
    # add column indicating scores and locations for each team
    df = team_scores(df)
    df['t1_win'] = np.where(df['t1_score'] > df['t2_score'], 1, 0)
    df['t1_marg'] = df['t1_score'] - df['t2_score']

    cols_keep = ['season', 'date', 'game_cat', 't1_team_id', 't2_team_id', 't1_score',
                 't2_score', 't1_win', 't1_marg']
    df = df.sort_index()
    df = df[cols_keep].reset_index()
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
    
def convert_game_scores(df):
    df = Match.id_from_name(df, 'team_tcp', 'away_team', drop=False)
    df = Match.id_from_name(df, 'team_tcp', 'home_team', drop=False)

    df['game_cat'] = "NA"
    df['season'] = map(Clean.season_from_date, df['date'].values)
    # convert columns to apply neutral id function
    df = game_box_convert(df)
    return df
