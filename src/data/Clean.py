"""Data cleaning.

This module contains functions used to clean data. Functions in this module
are used in other scripts/modules for general purpose data 
cleaning such as file manipulation, extracting, recoding, or reformatting. 

This script requires `pandas`, `numpy`, and `fuzzywuzzy` packages. 
It uses base Python packages `os`, `re`, and `datetime`. 

"""
import pandas as pd
import numpy as np
import os
import re
import datetime
from fuzzywuzzy import process
import Transfer

def write_file(df, data_out, file_name, keep_index=False):
    """Specify location and save .csv data file in one line.
    
    Parameters
    ----------
    df: DataFrame
        Data to save to file.
    data_out: str
        The relative path of location to save file.
    file_name: str
        Name of the file to save.
    keep_index: bool
        Whether to keep or drop the dataframe index. 
    """
    # combine the relative path and file name
    file = "".join([data_out, file_name, '.csv'])
    # save file
    df.to_csv(file, index=keep_index)


def list_of_files(directory, tag = None, tag_drop = None):
    """Returns list of all files in a directory. Names of files in returned list 
    include relative path. 
    
    Arguments
    ----------
    directory: string
        Relative path of directory containing the files.
    tag: string
        Optional, use to restrict list to files containing the tag.
    tag_drop: string
        Optional, use to exclude files containing the tag.
    """    
    # collect names of all files in directory
    file_names = os.listdir(directory)
    
    # if tag given, select file names that include tag
    if tag is not None:
        file_names = [x for x in file_names if tag in x]
    
    # if tag_drop given, remove files with tag
    if tag_drop is not None:
        file_names = [x for x in file_names if tag_drop not in x]
    
    # list of full file names
    files = [directory + x for x in file_names]
    
    return files

def list_of_filenames(directory):
    """Returns list of all file names in a directory with file type removed.
    
    Arguments
    ----------
    directory: string
        Relative path of directory containing the files.
    """    
    # collect names of all files in directory
    files = os.listdir(directory)
    
    # lambda function to obtain file name without file type
    file_stub = lambda x: x.split('.')[0]
    
    # iterate function over full file names
    file_names = [file_stub(x) for x in files]
    
    return file_names


def combine_files(directory, index_col=False, tag = None):
    """Combine data from all files in a directory.
    
    Arguments
    ----------
    directory: string
        Relative path of directory containing the files.
    index_col: boolean
        Optional, indicates whether to use the first column in files as index.
    tag: string
        Optional, use to restrict data to files containing the tag.
    
    """
    # list of file names with relative path 
    files = list_of_files(directory, tag = tag)
    
    # combine all dataframes
    data_list = [pd.read_csv(x, index_col=index_col) for x in files]
    df = pd.concat(data_list, sort=False)
    
    # return data
    return df


def school_name(x):
    """Format school names from external sources for joining with team id."""
    # remove capitals
    x = str.lower(x)
    # remove symbols and whitespace
    x = re.sub('[().&*\']', '', x)
    x = x.rstrip()
    # replace spaces with hyphens
    # this is the format used in team id file
    x = re.sub(r'  ', '-', x)
    x = re.sub(r' ', '-', x)
    # return the formatted string
    return x


def fuzzy_match(x, options, cutoff=85, with_score=False):
    """Indentify the closest match between a given string and a list of
    strings.
    
    Arguments
    ----------
    x: string
        The string to attempt to match.
    options: list
        The list of available options to match to.
    cutoff: integer
        In range of 1-99, a minimum value the match must exceed to keep the 
        matched string. Higher values indicate a closer match. 
    
    """
    # return the matched string and the match score
    best_match, match_score = process.extractOne(x, options)

    if with_score == True:
        return (best_match, match_score)
    else:
    # if matched string is at or above cutoff, return
        if match_score >= cutoff:
            return best_match
        # otherwise return None
        else:
            return None

def year4_from_string(s):
    """Returns numeric 4-digit year from string containing 2-digit year."""
    # extract digits from string
    year2 = "".join(re.findall('\d+', s))
    # default century is 2000
    pre = '20'
    # if final 2 year digits 80 or more, change prefix to 19
    if int(year2) > 80:
        pre = '19'
    # create 4-digit numeric year
    year4 = int(''.join([pre, year2]))
    return year4
    

def game_date(row):
    """Apply along rows to return the string date of each game. Row must contain 
    'daynum' integer for day of game and 'dayzero' string indicating the 'zero 
    day' for that year."""
    # identify integer day of game
    dn = row['daynum']
    # date object of the 'zero day' for that row
    dz = datetime.datetime.strptime(row['dayzero'], '%m/%d/%Y')
    # get date object for the date of the game
    date = dz + datetime.timedelta(days=dn)
    # convert date object to string format, inserting '_'
    date_id = date.strftime("%Y/%m/%d")
    # return string
    return date_id


def get_integer(x):
    """Returns the integer value from a string."""
    x_num = int(re.sub(r'\D+', '', x))
    return x_num


def upset_features(df):
    """Returns the dataframe with features re-aligned for upset prediction, where
    't1_' columns represent features for underdog teams and 't2_' columns 
    represent features for favored teams. 
    
    Arguments
    ----------
    df: pandas dataframe
        Contains data on both teams in matchup. Requires 't1_seed' and 't2_seed'
        indicating the seeds for both teams in matchup.
    """
    # want to align all underdog features under 't1_' label
    # where t1_seed is lower, t1 is favorite
    # create boolean array indicating rows to switch
    toswitch = df['t1_seed'] < df['t2_seed']
    
    # new copy of data to overwrite
    dfr = df.copy()
    
    # separate lists of features for 't1_' and 't2_'
    t1_cols = [x for x in dfr.columns if x[0:3] == 't1_']
    t2_cols = [x for x in dfr.columns if x[0:3] == 't2_']
    
    # set new values for each column where toswitch is true
    for t1_col, t2_col in zip(t1_cols, t2_cols):
        dfr.loc[toswitch, t1_col] = df.loc[toswitch, t2_col]
        dfr.loc[toswitch, t2_col] = df.loc[toswitch, t1_col]
    
    return dfr


def ids_from_index(df, full_date = False):
    """Returns the input dataframe with team id columns added. Team id numbers 
    are extracted from the dataframe index. Useful when team identifers have 
    been removed from data (i.e., for model training) but need to be 
    re-inserted for some reason, such as merging with other team data. 
    
    Arguments
    ----------
    df: pandas dataframe
        Requires an index of unique game identifers that contain team id for 
        both teams in the game.
    """
    # ensure index has name
    df.index = df.index.rename('game_id')
    # set index as column
    df = df.reset_index()
    
    # assume game date contains year only
    if full_date == False:
        df['t1_team_id'] = df['game_id'].apply(lambda x: int(x[5:9]))
        df['t2_team_id'] = df['game_id'].apply(lambda x: int(x[10:]))
    # if full date, need 
    else:
        df['t1_team_id'] = df['game_id'].apply(lambda x: int(x[11:15]))
        df['t2_team_id'] = df['game_id'].apply(lambda x: int(x[16:]))
    
    # return game identifer to index
    df = df.set_index('game_id')
    
    return df


def add_team_name(df, datdir='../data/'):
    """Returns the input dataframe with team names added. Team names are read 
    in from a file and merged with the input data using team identifers.
    
    Arguments
    ----------
    df: pandas dataframe
        Requires team identifer columns 't1_team_id' and 't2_team_id'. 
    datadir: string
        Relative path to data directory.
    """    
    # specificy path to team name data and read in dataframe
    path = "".join([datdir, 'scrub/teams.csv'])
    nm = pd.read_csv(path)
    
    nm = nm[['team_id', 'team_name']]
    
    # merge and create name column for team 1
    mrg = pd.merge(df, nm, left_on='t1_team_id', right_on='team_id',
                   how='inner')
    mrg = mrg.drop(columns=['team_id'])
    mrg = mrg.rename(columns={'team_name': 'team_1'})
    
    # merge and create name column for team 2
    mrg = pd.merge(mrg, nm, left_on='t2_team_id', right_on='team_id',
                   how='inner')
    mrg = mrg.drop(columns=['team_id'])
    mrg = mrg.rename(columns={'team_name': 'team_2'})
    
    
    return mrg


def switch_ids(df, toswitch):
    """Returns the input dataframe with team identifers switched in specified
    rows as indicated by input boolean array. Useful when the intent is to 
    organize data for presentation, such as when aligning all underdogs.
    
    Arguments
    ----------
    df: pandas dataframe
        Requires team identifer columns 't1_team_id' and 't2_team_id'. 
    toswitch: array
        Contains boolean values where True indicates rows to switch.
    """
    # copy of data for replacing values    
    dfr = df.copy()
    # switch both team identifers
    dfr.loc[toswitch, 't1_team_id'] = df.loc[toswitch, 't2_team_id']
    dfr.loc[toswitch, 't2_team_id'] = df.loc[toswitch, 't1_team_id']
    
    return dfr


def merge_from_list(df_list, merge_on, how='inner'):
    """Returns dataframe produced after merging all dataframes in input list. 
    Columns to use for merge provided as arguments, with 'inner' merge used 
    as default method. 
    
    Arguments
    ----------
    df_list: list
        Includes all dataframes desired to merge. 
    merge_on: list
        List of column names to use to merge rows.
    how: string
        The type of merge to perform. Default type is 'inner'. Other options
        include 'left', 'right', 'outer'
    """
    # use first df in list for left-most merge
    df = df_list[0]
    # repeat merge for remaining dfs in list
    for x in df_list[1:]:
        df = pd.merge(df, x, on=merge_on, how=how)
    
    return df


def round_floats(df, prec=2):
    """Returns dataframe with all float values rounded to specified precision. 
    
    Arguments
    ----------
    df: pandas dataframe
        Contains float numeric desired to be rounded. 
    prec: integer
        The desired decimal point precision for rounding. Default is 2. 
    """
    for c in df.columns:
        # only round columns of float data type
        if df[c].dtype == 'float':
            df[c] = df[c].round(decimals=prec)
    return df

    
def scrub_file(name, file_map):
    """Returns dataframe identified by file name with column names formatted and
    renamed according to file map. For files located in 'raw' subdirectory.
    """
    # create relative path to file and read data as dataframe
    file = '../data/raw/' + name + '.csv'
    df = pd.read_csv(file)
    
    # if file map has columns to rename, rename them
    if 'columns' in file_map[name].keys():
        df = df.rename(columns=file_map[name]['columns'])
    
    # column names all lower case for consistency across project
    df.columns = df.columns.str.lower()
    
    # fix unicode text in some team names
    if 'name_spelling' in df.columns:
        df['name_spelling'] = map(fix_unicode, df['name_spelling'].values)
    
    return df


def scrub_files(file_map, out='mysql', subset=[]):
    """Scrubs and writes all files identified in Constants file map.

    Arguments
    ----------
    file_map: dictionary
        Must contain key to match file names. Value is a dict that must contain
        'new_name' key paired with value as string of new file name. Dict may 
        contain 'columns' key indicating columns to rename.
    """
    # collect list of all files to process
    files = file_map.keys()
    
    # use subset to restrict file list
    if len(subset) != 0:
        files = [f for f in files if f in subset]

    # scrub and write each file
    for f in files:
        # obtain data with columns reformatted
        df = scrub_file(f, file_map)
        # get table name
        table_name = file_map[f]['new_name']
        # insert into mysql or save csv files
        if out == 'mysql':
            rows = Transfer.dataframe_rows(df)
            Transfer.insert(table_name, rows, at_once=True, create=False,
                            delete=True)
        else:
            data_out = '../data/scrub/'
            write_file(df, data_out, table_name, keep_index=False)

def date_range(start_date, end_date="today"):
    sds = start_date.split('/')
    sds = [int(x) for x in sds]
    sdate = datetime.date(sds[0], sds[1], sds[2])
    if end_date != "today":
        eds = end_date.split('/')
        eds = [int(x) for x in eds]
        edate = datetime.date(eds[0], eds[1], eds[2])
    else:
        edate = datetime.datetime.now().date()
    
    delta = edate - sdate
    
    dates = []
    
    for i in range(delta.days + 1):
        date = sdate + datetime.timedelta(days=i)
        date = date.strftime("%Y/%m/%d")
        dates.append(date)
    
    return dates

def date_plus(date, days):
    date_to = datetime.datetime.strptime(date, "%Y/%m/%d")
    next_to = date_to + datetime.timedelta(days=days)
    date_next = next_to.strftime("%Y/%m/%d")
    return date_next


def season_from_date(date):
    date_split = date.split('/')
    month = int(date_split[1])
    year = int(date_split[0])
    if month > 5:
        return year + 1
    else:
        return year

def fix_unicode(x):
    return x.decode('ascii', 'ignore')
