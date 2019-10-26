import pandas as pd
import os
import re
from fuzzywuzzy import process

def write_file(data, data_out, file_name, keep_index=False):
    """Set location and write new .csv file in one line."""
    file = "".join([data_out, file_name, '.csv'])
    data.to_csv(file, index=keep_index)

def combine_files(directory, index_col=False, tag = None):
    """Combine data from all files in a directory."""
    
    # collect names of all files in directory
    file_names = os.listdir(directory)
    
    # if tag given, select file names that include tag
    if tag is not None:
        files_names = [x for x in file_names if tag in x]
    
    # list of full file names for concatenation
    files = [directory + x for x in file_names]
    
    # combine all dataframes
    data_list = [pd.read_csv(x, index_col=index_col) for x in files]
    df = pd.concat(data_list, sort=False)
    
    return df

def clean_school_name(x):
    """Format school name for joining with other data."""
    x = str.lower(x)
    x = re.sub('[().&*\']', '', x)
    x = x.rstrip()
    x = re.sub(r'  ', '-', x)
    x = re.sub(r' ', '-', x)
    return x

def fuzzy_match(x, y):
    """Indentify the closest match between a given string and a list of
    strings."""
    best_match, score = process.extractOne(x, y)
    return best_match

def list_files(directory, suffix=".csv"):
    files = os.listdir(directory)
    return [filename for filename in files if filename.endswith(suffix)]

def add_season_column(x, directory):
    df = pd.read_csv(directory + x, index_col=None, header=0)
    if any([c in df.columns for c in ['Season', 'season']]):
        df = df.rename(columns={'Season': 'season'})
    else:
        year = re.findall('\d+', x)
        df['season'] = int('20' + ''.join(year))
    return df

def seed_to_numeric(seed):
    new_seed = int(re.sub(r'\D+', '', seed))
    return new_seed
