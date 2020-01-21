"""Data cleaning.

This module contains functions used to clean data. Functions in this module
may be used in multiple other scripts/modules for general purpose data 
cleaning such as file manipulation, extracting numeric data, recoding values, 
or reformatting strings. 

This script requires `pandas`, `numpy`, and `fuzzywuzzy`. It uses base 
modules `os`, `re`, and `datetime`. 

"""
import pandas as pd
import numpy as np
import os
import re
import datetime
from fuzzywuzzy import process

def write_file(df, data_out, file_name, keep_index=False):
    """Specify location and save .csv data file in one line.
    
    Arguments
    ----------
    df: pandas dataframe
        Data to save to file.
    data_out: string
        The relative path of location to save file.
    file_name: string
        Name of the file to save.
    keep_index: boolean
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
    keep_index: boolean
        Whether to keep or drop the dataframe index. 
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
    # replace spaces with hyphens, which is format used in team id file
    x = re.sub(r'  ', '-', x)
    x = re.sub(r' ', '-', x)
    # return the formatted string
    return x

def fuzzy_match(x, options, cutoff=85):
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
    # if matched string is at or above cutoff, return
    if match_score >= cutoff:
        return best_match
    # otherwise print message and return None
    else:
        print 'team not matched'
        # show the original string, the best match, and the score
        print x, best_match, match_score
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
    """Apply along rows to return the date of each game. Row must contain 
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
    date_id = date_id.replace('/', '_')
    # return string
    return date_id

def get_integer(x):
    """Returns the integer value from a string."""
    x_num = int(re.sub(r'\D+', '', x))
    return x_num

def upset_features(df):
    """Returns dataframe with features re-aligned for upset prediction, where
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


def ids_from_index(df):
    """Get team id numbers from the game id index."""
    df.index = df.index.rename('game_id')
    df = df.reset_index()
    df['t1_team_id'] = df['game_id'].apply(lambda x: int(x[5:9]))
    df['t2_team_id'] = df['game_id'].apply(lambda x: int(x[10:]))
    df = df.set_index('game_id')
    return df

def add_team_name(df, datdir='../data/'):
    """Add team names to dataset containing team id numbers."""
    path = "".join([datdir, 'scrub/teams.csv'])
    nm = pd.read_csv(path)
    ido = nm[['team_id', 'team_name']].copy()
    mrg = pd.merge(df, ido, left_on='t1_team_id', right_on='team_id',
                   how='inner')
    mrg = mrg.drop(columns=['team_id'])
    mrg = mrg.rename(columns={'team_name': 'team_1'})
    mrg = pd.merge(mrg, ido, left_on='t2_team_id', right_on='team_id',
                   how='inner')
    mrg = mrg.drop(columns=['team_id'])
    mrg = mrg.rename(columns={'team_name': 'team_2'})
    return mrg

def switch_ids(df, toswitch):
    dfr = df.copy()
    dfr.loc[toswitch, 't1_team_id'] = df.loc[toswitch, 't2_team_id']
    dfr.loc[toswitch, 't2_team_id'] = df.loc[toswitch, 't1_team_id']
    return dfr

def merge_from_list(df_list, merge_on, how='inner'):
    df = df_list[0]
    for x in df_list[1:]:
        df = pd.merge(df, x, on=merge_on, how=how)
    return df


def round_floats(df, prec=2):
    for c in df.columns:
        if df[c].dtype == 'float':
            df[c] = df[c].round(decimals=prec)
    return df
