"""Match team names to ID numbers.

This module contains functions used to match the names of schools from mixed
external data sources to a single numeric identifier from a master ID file. Separate 
functions have been created for pre-processing team names from different data
sources, as each source requires unique cleaning operations prior to attempting 
to match. 

School names from external sources contain exact and non-exact matches to names 
in the master file. This module uses a fuzzy string matching package to identify
numeric identifers for non-exact matches.

This module requires the `pandas` package. 
It imports the custom Clean module.

"""
import pandas as pd
import Clean

def clean_schools(datdir):
    """Returns a dataframe produced by combining data from files in the input 
    directory and cleaning school names. Used to pre-process school names for 
    matching with numeric team identifiers. 

    Arguments
    ----------
    datdir: string
        The relative path to subdirectory containing data files.
    """    
    # compiles all files into one dataset
    df = Clean.combine_files(datdir)

    # rows missing value for 'G' column are invalid
    df = df.dropna(subset=['G'])

    # isolate data to unique school names
    df = df[['School']].drop_duplicates()
    
    # add reformatted school name for better id matching
    df['team_clean'] = map(Clean.school_name, df['School'].values)
    
    # rename to create unique team identifer for source
    df = df.rename(columns={'School': 'team_ss'})    
    
    return df
    
def clean_kp(datdir):
    """Returns a dataframe produced by combining data from files in the input 
    directory and cleaning school names. Used to pre-process school names for 
    matching with numeric team identifiers. 

    Arguments
    ----------
    datdir: string
        The relative path to subdirectory containing data files.
    """
    # compiles all files into one dataset
    df = Clean.combine_files(datdir)

    # isolate data to unique names
    df = df[['TeamName']].drop_duplicates()

    # add reformatted school name for better id matching
    df['team_clean'] = df['TeamName'].str.lower()
    
    # replace specific teams that continually fail to match
    df['team_clean'] = df['team_clean'].str.replace('southwest texas st.', 'texas st.')
    
    # rename to create unique team identifer for source
    df = df.rename(columns={'TeamName': 'team_kp'})   
    
    return df

def match_id(df, id):
    """Returns a dataframe produced by matching team names in df to team names 
    in id. 

    Arguments
    ----------
    df: pandas dataframe
        Contains original team name and 'team_clean' column to attempt match.
    id: pandas dataframe
        Contains numeric team identifer and string team names in 
        'name_spelling' column.
    """       
    # identify number of unique schools for later comparison
    n_schools = df.shape[0]

    # join school name to id number in team identifer file 
    matched = pd.merge(df, id, how='inner', left_on='team_clean',
                       right_on=['name_spelling'])
    id_both = matched['team_id'].values
    
    # dataframe of non-merged school names
    nm = df[~df['team_clean'].isin(id['name_spelling'].values)].copy()
    nm_values = nm['team_clean'].values

    # list of team names from id df not merged
    # will use as list of options for fuzzy matching
    rem_teams = id[~id['team_id'].isin(id_both)]['name_spelling'].values
    
    # run fuzzy matching function on 'team_clean' for nonmerged schools
    nm_names = map(lambda x: Clean.fuzzy_match(x, rem_teams), nm_values)
    
    # add column of fuzzy matched names to nonmerged team df
    nm.loc[:, 'name_spelling'] = nm_names
    
    # join team id numbers using the fuzzy-matched names
    nm = pd.merge(nm, id, on='name_spelling', how='inner')

    # combine merged and fuzzy matched rows
    all = pd.concat([matched, nm], sort=False)
    
    # only keep the original name and the matched numeric id
    all = all.drop(columns=['team_clean', 'name_spelling'])
    
    # print warning if any school team_ids not identified
    if n_schools != all.shape[0]:
        n_left = n_schools - all.shape[0]
        print 'Warning: %d team ids not joined to school names.' % (n_left)

    return all

def create_key(datdir):
    """Creates a data file containing the unique team numeric identifer and 
    original team names for all matched data sources.

    Arguments
    ----------
    datdir: string
        Relative path to data directory.
    """ 
    # read in id and team spellings file
    id = pd.read_csv(datdir + '/scrub/team_spellings.csv')
    id = id[['team_id', 'name_spelling']]
    
    # clean schools data and match to numeric identifier
    schools = clean_schools(datdir + 'external/school_stats/')
    schools_id = match_id(schools, id)
    
    # clean team ratings data and match to numeric identifier
    kp = clean_kp(datdir + 'external/kp/')
    kp_id = match_id(kp, id)    

    # read in master id file
    key = pd.read_csv(datdir + '/scrub/teams.csv')
    key = key[['team_id', 'team_name']]
    
    # create universal key
    for df in [schools_id, kp_id]:
        key = pd.merge(key, df, on='team_id', how='left')

    # set location to write file and save file
    data_out = datdir + 'interim/'
    Clean.write_file(key, data_out, 'id_key')

def id_from_name(datdir, df, key_col, name_col, drop_name=True):
    """From input data containing team name column specified in 'name_col', 
    returns dataframe containing team numeric identifiers.

    Arguments
    ----------
    datdir: string
        Relative path to data directory.
    df: pandas dataframe
        Data input to add team numeric identifier as a column.
    key_col: string
        The name of column in id key file to match team name.
    name_col: string
        The name of team name column in the input df.
    """ 
    # read in the id key data
    id_file = datdir + '/interim/id_key.csv'
    id = pd.read_csv(id_file)
    # from id key data, only need numeric identifer and key_col to merge on
    id = id[['team_id', key_col]]
    # join data the id key file using specified inputs
    mrg = pd.merge(df, id, left_on=name_col, right_on=key_col, how='inner')
    # list of cols to drop, key_col is redundant with name_col
    drop_cols = [key_col]
    # add name_col to drop list, if desired
    if drop_name == True:
        drop_cols.append(name_col)
    # remove columns from dataframe
    mrg = mrg.drop([key_col, name_col], axis=1)
    
    return mrg
