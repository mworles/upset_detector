import pandas as pd
import numpy as np
from src.data.transfer import DBAssist
from src.data import clean


def summary_by_season(team_games):
    team_games = prep_stats_by_team(team_games)
    team_games = team_games.sort_values(['team_id', 'date'])
    team_summary = compute_summary(team_games)
    return team_summary


def tourney_performance(modifier=None):
    """Return dataframe with counts of team NCAA tournament wins and games."""

    # import results from previous tournaments
    dba = DBAssist()
    df = dba.return_data('ncaa_results', modifier=modifier)
    dba.close()

    # each row is one game, contains ids of both teams ('wteam', 'team')
    # create "win" indicator by separating winners and losers
    wteams = df[['season', 'wteam']]
    wteams = wteams.rename(columns={'wteam': 'team_id'})
    wteams['win'] = 1

    lteams = df[['season', 'lteam']]
    lteams = lteams.rename(columns={'lteam': 'team_id'})
    lteams['win'] = 0

    # combine data to create one row per team per game
    team_games = pd.concat([wteams, lteams], ignore_index=True)

    # get number of tourney wins and games for each team
    group_on = ['season', 'team_id']
    team_years = team_games.groupby(group_on)['win'].aggregate(['count', 'sum'])
    team_years = team_years.reset_index().rename(columns={'count': 'games',
                                                          'sum': 'wins'})

    return team_years


def games_regular():
    """Return one dataframe containing all regular season game data."""
    
    # initialize database assistant
    dba = DBAssist()

    # data contained in two tables with 'detailed' or 'compact' results
    # need both because 'detailed' only available >= season 2003
    reg_dtl = dba.return_data('reg_results_dtl')
    reg_com = dba.return_data('reg_results', modifier='WHERE season < 2003')    
    df = pd.concat([reg_dtl, reg_com], sort=False)
    df = df[df['season'] == 2018]
    
    dba.close()
    
    return df


def split_games_to_teams(df):
    """Returns dataframe of all game stats with one row per team."""
    # lists of team-specific statistical column names for winners and losers
    winner_cols = [x for x in df.columns if x[0] == 'w']
    loser_cols = [x for x in df.columns if x[0] == 'l']

    # get the original index of all original columns
    both = [list(df.columns).index(x) for x in ['season', 'daynum']]
    winner_index = [list(df.columns).index(x) for x in winner_cols]
    loser_index = [list(df.columns).index(x) for x in loser_cols]
    
    # map of all column indices to provide as input to parse_games function
    column_map = {'both': both, 'winner': winner_index, 'loser': loser_index}
    # make array for faster computation
    df_array = df.values

    # all games parsed twice to create row for winners and row for losers
    winners = list(map(lambda game: team_stats_from_game(game, column_map),
                       df_array))
    losers = list(map(lambda game: team_stats_from_game(game, column_map,
                                                        winner=False),
                      df_array))
    games = winners + losers

    # column names for new dataset to indicate the target team and opponent
    team_cols = ['team_' + c[1:] for c in winner_cols]
    opp_cols = ['opp_' + c[1:] for c in loser_cols]
    new_columns = ['season', 'daynum']
    new_columns.extend(team_cols + opp_cols)
    
    # apply new column names to games
    df = pd.DataFrame(games, columns=new_columns)
    df = df.rename(columns={'team_team': 'team_id', 'opp_team': 'opp_id'})
    
    # create win/loss indicators
    df['team_win'] = np.where(df['team_score'] > df['opp_score'], 1, 0)
    df['team_loss'] = np.where(df['team_win'] == 1, 0, 1)
    
    # sort data chronologically
    df = df.sort_values(['season', 'daynum', 'team_id'])

    return df


def team_stats_from_game(game_row, column_map, winner=True):
    """Extract stats for team and opponent depending on winner of game."""
    
    # use map of column indices to pull game stats
    both = game_row[column_map['both']].tolist()
    winner_stats = game_row[column_map['winner']].tolist() 
    loser_stats = game_row[column_map['loser']].tolist()
    
    # if team was the winner sequence winner stats first
    if winner==True:
        team_data = both + winner_stats + loser_stats
    else:
        team_data = both + loser_stats + winner_stats

    return team_data




def prep_stats_by_team(df):
    start_cols = df.columns.tolist()

    # add computed stats to data
    df = add_computed_stats(df)

    # add date to each game
    dba = DBAssist()
    seasons = dba.return_data('seasons')
    dba.close()
    
    df = clean.date_from_daynum(df, seasons)
    
    columns_to_keep = ['season', 'team_id', 'team_score', 'opp_id',
                       'opp_score', 'team_loss', 'team_win']
    
    # make list of new columns to keep data
    columns_added = [col for col in list(df.columns) if col not in start_cols]
    columns_added.remove('pos')
    columns_to_keep.extend(columns_added)
    df = df[columns_to_keep]

    return df


# code to use on team game row data
def add_computed_stats(df):
    # estimated possessions per team in game
    df['pos'] = ((df['team_fga'] + df['opp_fga']) +
                 0.44 * (df['team_fta'] + df['opp_fta']) -
                 (df['team_or'] + df['opp_or']) +
                 (df['team_to'] + df['opp_to'])) / 2
    # scoring margin
    df['scrmarg'] = df['team_score'] - df['opp_score']
    # free throw rate
    df['ftrat'] = df['team_fta'] / df['team_fga']
    # free throw made rate
    df['ftmrat'] = df['team_ftm'] / df['team_fga']
    # defensive free throw rate
    df['ftrat_d'] = df['opp_fta'] / df['opp_fga']
    # effective field goal percentage
    df['efgpct'] = (df['team_fgm'] + (0.5 * df['team_fgm3'])) / df['team_fga']
    # effective field goal percentage defense
    df['efgpct_d'] = (df['opp_fgm'] + (0.5 * df['opp_fgm3'])) / df['opp_fga']
    # defensive rebound percentage
    df['drbpct'] = df['team_dr'] / (df['team_dr'] + df['opp_or'])
    # offensive rebound percentage
    df['orbpct'] = df['team_or'] / (df['team_or'] + df['opp_dr'])
    # offensive rebound + turnover margin
    df['orbtomarg'] = ((df['team_or'] - df['team_to']) -
                       (df['opp_or'] - df['opp_to']))
    # turnover percentage
    df['topct'] = df['team_to'] / df['pos']
    # defensive turnover percentage
    df['topct_d'] = df['opp_to'] / df['pos']

    return df


def compute_summary(df):
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

    mrg3['winpct'] = mrg3['wins'] / (mrg3['wins'] + mrg3['losses'])
    mrg3 = mrg3.drop(columns=['wins', 'losses'])
    
    return mrg3


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


def sum_stats(gb):
    cols_to_sum = ['team_win', 'team_loss']
    tgb_sum = gb[cols_to_sum].sum()
    tgb_sum.columns = ['wins', 'losses']
    tgb_sum = tgb_sum.reset_index()
    return tgb_sum


def pct_last(group, n=5):
    #Compute win percentage in last n games.
    wins = group.iloc[-n:].sum()
    return wins / float(n)


def streak(y):
    #Compute number of consecutive events.
    streak = y * (y.groupby((y != y.shift()).cumsum()).cumcount() + 1)
    return int(streak[-1:])
