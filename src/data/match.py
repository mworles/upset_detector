"""Match team name to ID number.

This script contains functions used to match the names of schools from mixed
data sources to a single numeric identifier from a master ID file. 

The raw data names contain exact and non-exact matches to names in the master 
file. This script uses a fuzzy string matching package to identify IDs for 
non-exact matches.

This script requires `pandas` and `fuzzywuzzy`. It imports the custom Clean
module to use data cleaning functions.

"""
import pandas as pd
import Clean

def clean_schools(dir):
    
    # compiles all files into one dataset
    df = Clean.combine_files(dir)

    # rows missing value for 'G' column are invalid
    df = df.dropna(subset=['G'])

    # isolate data to unique school names
    df = df[['School']].drop_duplicates()

    # add clean school name for id matching
    df['team_clean'] = map(Clean.school_name, df['School'].values)
    
    return df
    
def clean_kp(dir):
    df = Clean.combine_files(dir)

    # isolate data to unique names
    df = df[['TeamName']].drop_duplicates()

    # modify kp team names to match kaggle format and improve matching
    df['team_clean'] = df['TeamName'].str.lower()
    df['team_clean'] = df['team_clean'].str.replace('southwest texas st.', 'texas st.')
    
    return df

def match_id(df, dir):
    
    # number of schools for later comparison
    n_schools = df.shape[0]
    
    file_id = dir + '/scrub/team_spellings.csv'
    ns = pd.read_csv(file_id)
    
    # join school name to id number in team identifer file 
    matched = pd.merge(df, ns, how='inner', left_on='team_clean',
                       right_on=['name_spelling'])
    id_both = matched['team_id'].values
    
    # dataframe of non-matched school names
    nm = df[~df['team_clean'].isin(ns['name_spelling'].values)].copy()
    nm_values = nm['team_clean'].values

    # list of remaining team names
    rem_teams = ns[~ns['team_id'].isin(id_both)]['name_spelling'].values
    
    # run function on school column for nonmatched frame
    nm_names = map(lambda x: Clean.fuzzy_match(x, rem_teams), nm_values)
    nm.loc[:, 'name_spelling'] = nm_names
    
    # join team ids using the fuzzy-matched names
    nm = pd.merge(nm, ns, on='name_spelling', how='inner')

    # combine files
    all = pd.concat([matched, nm])
    all = all.drop(columns=['team_clean', 'name_spelling'])
    
    # print warning if any school team_ids not identified
    if n_schools != all.shape[0]:
        n_left = n_schools - all.shape[0]
        print 'Warning: %d team ids not joined to school names.' % (n_left)
    
    all = all.rename(columns={'School': 'team_ss',
                              'TeamName': 'team_kp'})
    
    return all

def match_schools(dir, write=False):
    data_in = dir + 'external/school_stats/'
    schools = clean_schools(data_in)
    df = match_id(schools, dir)
    if write==True:
        data_out = dir + 'interim/'
        Clean.write_file(df, data_out, 'id_ss')
    return df

def match_kp(dir, write=False):
    data_in = dir + 'external/kp/'
    schools = clean_kp(data_in)
    df = match_id(schools, dir)
    if write==True:
        data_out = dir + 'interim/'
        Clean.write_file(df, data_out, 'id_kp')
    return df

def create_key(dir):
    id = pd.read_csv(dir + '/scrub/teams.csv')
    id = id[['team_id', 'team_name']]
    ss = match_schools(dir)
    kp = match_kp(dir)
    
    for df in [ss, kp]:
        id = pd.merge(id, df, on='team_id', how='left')

    data_out = dir + 'interim/'
    # save  data file
    Clean.write_file(id, data_out, 'id_key')
