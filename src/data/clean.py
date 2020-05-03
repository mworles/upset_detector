""" clean.

A module containing custom functions used to clean, transform, or rearrange
project data.

Functions
---------
list_of_files
combine_files
fuzzy_match
dates_in_range
date_after_interval
check_date
season_of_date
make_game_id
date_from_daynum
order_team_ids
team_scores
map_teams
team_score_map
team_site_map
has_columns

"""
import pandas as pd
import os
import datetime
from fuzzywuzzy import process
from transfer import DBAssist

def combine_files(path, tag = None, index_col=False):
    """
    Return dataframe produced by combining tabular data files into
    a single table.
    
    Parameters
    ----------
    path: string
        Path to directory containing the files.
    tag: str, optional, default None
        If given, restrict file list to files containing the tag.
    index_col: bool, optional, default False
        If True, use first column in the file as dataframe index.

    Returns
    -------
    df : pandas DataFrame
        Dataframe resulting from combining the files.
    
    """
    # combine all dataframes
    file_list = list_of_files(path, tag=tag)
    df_list = [pd.read_csv(x, index_col=index_col) for x in file_list]
    df = pd.concat(df_list, sort=False)

    return df


def list_of_files(path, tag = None):
    """
    Return list of all files in a directory. 
    
    Parameters
    ----------
    relative_path: string
        Relative path to directory containing the files.
    tag: str, optional, default None
        If given, restrict file list to files containing the tag.

    Returns
    -------
    files : list of str
        List of all files with each element containing the relative path.

    """
    # collect names of all files in directory
    file_names = os.listdir(path)

    # if tag given, select file names that include tag
    if tag is not None:
        file_names = [x for x in file_names if tag in x]

    # add directory path to files
    files = [path + x for x in file_names]

    return files


def fuzzy_match(target, options, cutoff=85, with_score=False):
    """
    Return fuzzy match for a target string from a list of options. 
    
    Parameters
    ----------
    target: str
        The string to attempt to match.
    options: list of str
        The list of available options to match to.
    cutoff: int, optional, min 1, max 100
        Minimum Levenshtein distance similarity ratio required to return
        the result. If cutoff not met, returns None. 
    with_score: bool, optional, default False 
        Ignore cutoff and return tuple containing top result and score.

    Returns
    -------
    result : str or tuple of (str, int)
        Best-matching str, or tuple with best matching str and the score.

    """
    # extract best-matching string and the distance similarity ratio
    best_match, match_score = process.extractOne(target, options)
    
    # cutoff ignored if with_score set to True
    if with_score == True:
        result = (best_match, match_score)
    else:
    # use matched string only if score >= cutoff
        if match_score >= cutoff:
            result = best_match
        else:
            result = None

    return result


def dates_in_range(start_date, end_date="today"):
    """
    Return list of str dates in the supplied range, endpoints inclusive.

    Parameters
    ----------
    start_date: str
        Starting date for the desired date range.
    end_date: str, optional, default is today's date
        Ending date for the desired range.

    Returns
    -------
    dates : list of str
        All dates in the supplied range.

    """
    dt_start = check_date(start_date)
        
    if end_date != "today":
        dt_end = check_date(end_date)
    else:
        dt_end = datetime.datetime.now().date()
    
    delta = dt_end - dt_start

    date_strings = []

    for i in range(delta.days + 1):
        date = dt_start + datetime.timedelta(days=i)
        date_string = date.strftime("%Y/%m/%d")
        date_strings.append(date_string)

    return date_strings


def date_after_interval(interval, start_date='today'):
    """
    Return str date that occurs after the supplied interval.

    Parameters
    ----------
    interval: int
        Number of days to compute the interval.
    start_date: str, optional, default is today's date
        Baseline date for the interval.

    Returns
    -------
    date_interval : str
        Date that occurs at the end of the interval

    """
    # validate the correct date format, get datetime object
    if start_date != "today":
        dt_start = check_date(start_date)
    else:
        dt_start = datetime.datetime.now().date()
    
    dt_interval = dt_start + datetime.timedelta(days=interval)
    date_interval = dt_interval.strftime("%Y/%m/%d")
    
    return date_interval


def season_of_date(date):
    """
    Return baskeball season extracted from string date.

    Parameters
    ----------
    date: str
        Str of date to extract season from.

    Returns
    -------
    season : int
        The 4-digit year used to identify season the date occurred.

    """
    # validate that given date is in correct format
    test_date = check_date(date)
    month, year = test_date.month, test_date.year

    # season is next calendar year for any games occurring later than april
    if month > 4:
        season = year + 1
    else:
        season = year

    return season


def check_date(date_string):
    """
    Return datetime date or raise exception if format isn't correct.

    Parameters
    ----------
    date: str
        Str of date to check.

    Returns
    -------
    dt_date : datetime.date
        Datetime.date object from str date.

    """
    try:
        dt_date = datetime.datetime.strptime(date_string, "%Y/%m/%d").date()
        return dt_date
    except ValueError as e:
        raise Exception(e)


def date_from_daynum(df):
    """
    Return dataframe with string date computed from 'daynum' column.

    Parameters
    ----------
    df: pandas dataframe
        Must contain 'season' and 'daynum' columns.

    Returns
    -------
    df: pandas dataframe
        Dataframe with 'date' column containing string date.

    """
    # test if df contains required columns
    has_columns(df, ['daynum', 'season'])

    # import season data that has date of zero daynum for each season
    dba = DBAssist()
    seas = dba.return_data('seasons')
    dba.close()

    # season dayzero to datetime to compute deltas
    dt_zero = map(lambda x: datetime.datetime.strptime(x, '%m/%d/%Y'),
                  seas['dayzero'])
    season_map = {k: v for k, v in zip(seas['season'].values, dt_zero)}
    
    season_day = zip(df['season'].values, df['daynum'].values)

    def compute_date(season_daynum, season_map):
        dt_zero = season_map[season_daynum[0]]
        dt_date = dt_zero + datetime.timedelta(days=season_daynum[1])
        date = dt_date.strftime("%Y/%m/%d")
        return date

    df['date'] = map(lambda x: compute_date(x, season_map), season_day)

    return df


def make_game_id(df):
    """
    Return dataframe with a distinct game identifier column constructed from
    team numeric identifers and date of game.

    Parameters
    ----------
    df: pandas dataframe
        Must contain two numeric team identifer columns and a date column.
        
    Returns
    -------
    df: pandas dataframe
        Dataframe with distinct game identifer in 'game_id' column.

    """
    # ensure data has ordered numeric team identifiers
    need = ['t1_team_id', 't2_team_id', 'date']
    has_all = all(elem in df.columns for elem in need)
    assert(has_all is True), "df must contain {}".format(", ".join(need))
    
    # reformat date to use underscore separator
    date_under = df['date'].str.replace('/', '_')
    
    # need team numeric identifier ints as strings
    id_lower = df['t1_team_id'].astype(int).astype(str)
    id_upper = df['t2_team_id'].astype(int).astype(str)
    
    # game_id is date combined with both teams
    df['game_id'] = date_under + '_' + id_lower + '_' + id_upper
    
    # return data with new index
    return df


def order_team_ids(df, id_cols):
    """
    Return dataframe with ordered numeric team identifiers.

    Parameters
    ----------
    df: pandas dataframe
        Must contain two numeric team identifer columns.
    id_cols: list of str, length == 2
        Names of the original numeric team identifier columns.

    Returns
    -------
    df: pandas dataframe
        Dataframe with team id columns 't1_team_id' and 't2_team_id' where 
        't1_team_id' contains numerically lower id.

    """
    # ensure input contains 2 team id columns
    assert(len(id_cols) == 2), "Must input 2 team id columns."
    
    # use min and max to create new identifiers
    df['t1_team_id'] = df[id_cols].min(axis=1).astype(int)
    df['t2_team_id'] = df[id_cols].max(axis=1).astype(int)
    
    return df


def map_teams(df, team_map, col_name):
    """
    Return dataframe with team values from map assigned to new columns.
    
    Parameters
    ----------
    df: pandas dataframe
        Must contain team identifer columns 't1_team_id' and 't2_team_id'.
    team_map: dict
        Dict keys must match index of df. Values are dicts with team numeric
        identifiers as keys and team-specific values.
    col_name : str
        The suffix to assign the new column names.

    Returns
    -------
    df : pandas dataframe
        The input dataframe with team values assigned to 2 new columns.
    
    """
    has_columns(df, ['t1_team_id', 't2_team_id'])
    team1_team2 = zip(df.index.values, df['t1_team_id'], df['t2_team_id'])
    t1_col = 't1_' + col_name
    t2_col = 't2_' + col_name
    df[t1_col] = map(lambda x: team_map[x[0]][x[1]], team1_team2)
    df[t2_col] = map(lambda x: team_map[x[0]][x[2]], team1_team2)
    return df


def team_score_map(df):
    """
    Return dict mapping game index to scores for each team.

    Parameters
    ----------
    df: pandas dataframe
        Must contain columns 'wteam', 'wscore', 'lteam', 'lscore' to indicate
        winner, winning score, loser, losing score.

    Returns
    -------
    team_map : dict
        Dict with df index as keys. Values are dicts with team numeric
        identifiers as keys and team scores as values.

    """    
    # test if df contains required columns
    has_columns(df, ['wteam', 'wscore', 'lteam', 'lscore'])
    wmap = zip(df.index.values, df['wteam'].values, df['wscore'].values)
    lmap = zip(df.index.values, df['lteam'].values, df['lscore'].values)
    team_map = {game: {team: score} for game, team, score in wmap}
    loser_map = {game: {team: score} for game, team, score in lmap}
    [team_map[game].update(loser_map[game]) for game in loser_map.keys()]
    return team_map


def team_site_map(df):
    """
    Return dict mapping game index to site (H, A, or N) for each team.

    Parameters
    ----------
    df: pandas dataframe
        Must contain columns 'wteam', lteam', 'wloc' to indicate
        winner, loser, winner location.

    Returns
    -------
    team_map : dict
        Dict with df index as keys. Values are dicts with team numeric
        identifiers as keys and team site as values.

    """    
    has_columns(df, ['wteam', 'lteam', 'wloc'])
    wmap = zip(df.index.values, df['wteam'].values, df['wloc'].values)
    lteam_dict = {'A': 'H', 'H': 'A', 'N': 'N'}
    lloc = [lteam_dict[win_loc] for win_loc in df['wloc'].values]
    lmap = zip(df.index.values, df['lteam'].values, lloc)
    game_dict = {game: {team: loc} for game, team, loc in wmap}
    lose_dict = {game: {team: loc} for game, team, loc in lmap}
    [game_dict[game].update(lose_dict[game]) for game in lose_dict.keys()]    
    return game_dict


def has_columns(df, columns):
    """Assert that a dataframe contains all of the lised columns."""
    has_all = all(elem in df.columns for elem in columns)
    assert(has_all is True), "df must contain {}".format(", ".join(columns))
