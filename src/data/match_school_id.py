import os
import pandas as pd
import sys
sys.path.append("../")
from Cleaning import write_file, combine_files, clean_school_name, fuzzy_match
from Constants import COLUMNS_TO_RENAME

def clean_schools(directory):
    df = combine_files(directory)

    # rows missing value for 'G' column are invalid placeholders
    df = df.dropna(subset=['G'])

    # isolate data to unique school names
    df = df[['School']].drop_duplicates()

    # count to compare below for success of matching
    n_schools = df.shape[0]

    # add clean school name for id matching
    df['school_clean'] = map(clean_school_name, df['School'].values)
    
    return df

def match_school_id(df):
    
    # number of schools for later comparison
    n_schools = df.shape[0]
    
    # import team id number data
    ns = pd.read_csv('../../data/raw/TeamSpellings.csv')
    # rename some columns for code compatibility
    ns = ns.rename(columns=COLUMNS_TO_RENAME)

    # join school name to id number in team identifer file 
    matched = pd.merge(df, ns, how='inner', left_on=['school_clean'],
                       right_on=['name_spelling'])
    id_both = matched['team_id'].values

    # dataframe of non-matched school names
    nm = df[~df['school_clean'].isin(ns['name_spelling'].values)].copy()
    nm_values = nm['school_clean'].values

    # list of remaining team names
    rem_teams = ns[~ns['team_id'].isin(id_both)]['name_spelling'].values

    # run function on school column for nonmatched frame
    nm_names = map(lambda x: fuzzy_match(x, rem_teams), nm_values)
    nm.loc[:, 'name_spelling'] = nm_names
    
    # join team ids using the fuzzy-matched names
    nm = pd.merge(nm, ns, on='name_spelling', how='inner')

    # combine files
    all = pd.concat([matched, nm])
    all = all.drop(columns=['school_clean', 'name_spelling'])
    
    # print warning if any school team_ids not identified
    if n_schools != all.shape[0]:
        n_left = n_schools - all.shape[0]
        print 'Warning: %d team ids not joined to school names.' % (n_left)
    
    return all

print "running %s" % (os.path.basename(__file__))

directory = 'data/external/school_stats/'
schools = clean_schools(directory)
df = match_school_id(schools)
df = df.rename(columns={'School': 'ss_team'})

data_out = '../../data/interim/'

# save school stats data file
write_file(df, data_out, 'id_school')
