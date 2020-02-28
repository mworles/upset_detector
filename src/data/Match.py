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
import numpy as np
import Clean
import Odds
import Transfer
import re

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
    df['team_clean'] = map(lambda x: Clean.school_name(x), df['TeamName'])

    # rename to create unique team identifer for source
    df = df.rename(columns={'TeamName': 'team_kp'})   
    
    return df

def clean_odds_portal(datdir):
    """Returns a dataframe produced by combining data from files in the input 
    directory and cleaning school names. Used to pre-process school names for 
    matching with numeric team identifiers. 

    Arguments
    ----------
    datdir: string
        The relative path to subdirectory containing data files.
    """
    data = Odds.parse_oddsportal(datdir)
    col_names = ['date', 'team_1', 'team_2', 'odds1', 'odds2']
    df = pd.DataFrame(data, columns=col_names)

    # list of all unique team names
    teams = list(set(list(df['team_1']) + list(df['team_2'])))
    
    df = pd.DataFrame({'team_oddsport': teams})
    df = df[df['team_oddsport'].notnull()]
    
    
    df['team_clean'] = map(lambda x: Clean.school_name(x), df['team_oddsport'])
    
    return df

def clean_pt(datdir):

    df = Clean.combine_files(datdir)

    # list of all unique team names
    teams = list(set(list(df['home']) + list(df['road'])))

    df = pd.DataFrame({'team_pt': teams})
    df = df[df['team_pt'].notnull()]

    df['team_clean'] = map(lambda x: Clean.school_name(x), df['team_pt'])

    return df


def clean_sbro(datdir):
    def split_caps(x):
        result = re.findall('[A-Z][^A-Z]*', x)
        if len(result) == 0:
            return x
        else:
            return result
    
    def join_team(team_list):
        team_full = ''
        for w in team_list:
            if len(w) == 1:
                pass
            else:
                if team_full != '':
                    w = ' ' + w + ' '
                else:
                    w = w + ' '
            team_full += w
        return team_full

    df = pd.read_csv(datdir + 'external/sbro/spreads_sbro.csv')
    
    # list of all unique team names
    teams = list(set(list(df['home']) + list(df['away'])))

    df = pd.DataFrame({'team_sbro': teams})
    df = df[df['team_sbro'].notnull()]
    team_split = [split_caps(x) for x in df['team_sbro']]
    team_full = map(join_team, team_split)
    df['team_clean'] = map(lambda x: Clean.school_name(x), team_full)

    return df

def clean_tcpalm(table_name):
    dba = Transfer.DBAssist()
    dba.connect()
    table = dba.return_table(table_name)
    df = pd.DataFrame(table[1:], columns=table[0])
    
    # all unique team names
    teams = list(set(list(df['home_team']) + list(df['away_team'])))
    teams = [t for t in teams if t is not None]
    
    df = pd.DataFrame({'team_tcp': teams})
    df['team_clean'] = map(lambda x: Clean.school_name(x), df['team_tcp'])
    
    return df
    
def clean_vi(table_name):
    dba = Transfer.DBAssist()
    dba.connect()
    table = dba.return_table(table_name)
    df = pd.DataFrame(table[1:], columns=table[0])
    
    # all unique team names
    teams = list(set(list(df['team_1']) + list(df['team_2'])))
    teams = [t for t in teams if t is not None]
    
    source_col = 'team_vi_' + table_name
    df = pd.DataFrame({source_col: teams})
    df['team_clean'] = map(lambda x: Clean.school_name(x), df[source_col])
    
    return df


def match_team(source, id, min_year=None):
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
    if min_year is not None:
        id = id[id['lastd1season'] >= min_year]

    # identify number of unique schools for later comparison
    n_schools = len(pd.unique(id['team_id']))
    source_col = [x for x in source.columns if x != 'team_clean'][0]
    
    # join school name to id number in team identifer file 
    merged = pd.merge(id, source, how='outer', left_on='name_spelling',
                       right_on=['team_clean'])
    matched = merged.dropna().drop_duplicates()    
    nm_id = merged[merged['team_clean'].isnull()]
    
    nm_id = nm_id.drop([source_col], axis=1)
    # remove where team id was matched
    nm_id = nm_id[~nm_id['team_id'].isin(matched['team_id'].values)]
    
    nm_source = merged[merged['team_id'].isnull()]
    
    keep_cols = ['team_id', source_col]
    matched = matched[keep_cols]
    matched = matched.dropna()
    matched = matched.drop_duplicates()
    
    if nm_source.shape[0] == 0:
        df = matched.copy()
    else:
        nm_values = nm_id['name_spelling'].values

        # list of team names from source df not merged
        # will use as list of options for fuzzy matching
        rem_teams = nm_source['team_clean'].values
        
        # run fuzzy matching function on 'team_clean' for nonmerged schools
        matches_scores = map(lambda x: Clean.fuzzy_match(x, rem_teams, with_score=True), nm_values)
        nm_id.loc[:, 'team_clean'] = [x[0] for x in matches_scores]
        nm_id.loc[:, 'score'] = [x[1] for x in matches_scores]
        
        nmgb = nm_id.groupby('team_id')
        nmgb = nmgb.apply(lambda x: x.sort_values(['score'], ascending=False))
        nmgb = nmgb.reset_index(drop=True).groupby('team_id').head(1)
        
        matches = []
        index_vals = nmgb.index.values
        
        print ''
        print '%s D1 numeric ids not merged' % (len(index_vals))
        for v in index_vals:
            print ''
            print 'ID name:         %s' % (nmgb.loc[v, 'name_spelling'])
            print 'Source name:     %s' % (nmgb.loc[v,'team_clean'])
            match = str.upper(raw_input('Match? Y or N: '))
            matches.append(match)

        nmgb['match'] = matches
        
        nmgb['team_clean'] = np.where(nmgb['match'] == 'Y', nmgb['team_clean'], None)
        nmgb = pd.merge(nmgb, source, left_on='team_clean', right_on='team_clean',
                        how='left')
        df = pd.concat([matched, nmgb[keep_cols]])
        
        
        source_teams = pd.unique(source[source_col])
        source_nm = [t for t in source_teams if t not in df[source_col].values]
        print '%s source teams not matched' % (len(source_nm))
        print source_nm

    return df

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
    ido = id.copy()
    
    teams = pd.read_csv('../data/scrub/teams.csv')
    id = pd.merge(id, teams, how='inner', left_on='team_id', 
                  right_on='team_id')
    
    id['name_spelling'] = id['name_spelling'].str.replace(' ', '-')
    id['name_spelling'] = id['name_spelling'].str.replace('.', '')
    
    # clean schools data and match to numeric identifier
    schools = clean_schools(datdir + 'external/school_stats/')
    print 'matching school stats'
    schools_id = match_team(schools, id, min_year=1993)
    key_list = [schools_id]
    
    # clean team ratings data and match to numeric identifier
    kp = clean_kp(datdir + 'external/kp/')
    print 'matching kenpom'
    kp_id = match_team(kp, id, min_year=2002)
    key_list.append(kp_id)
    
    # clean and match oddsportal odds teams
    op = clean_odds_portal(datdir + 'external/odds/')
    print 'matching oddsportal'
    op_id = match_team(op, id, min_year=2009)
    key_list.append(op_id)
    
    # clean and match predictiontracker teams
    pt = clean_pt(datdir + 'external/pt/')
    print 'matching predictiontracker'
    pt_id = match_team(pt, id, min_year=2003)
    key_list.append(pt_id)
    
    # clean and match sportsbookreviews teams
    sbro = clean_sbro(datdir)
    print 'matching sportsbookreviews'
    sbro_id = match_team(sbro, id, min_year=2008)
    key_list.append(sbro_id)
    
    # clean tcpalm teams
    tcp = clean_tcpalm('game_scores')
    print 'matching tcpalm'
    tcp_id = match_team(tcp, id, min_year=2019)
    key_list.append(tcp_id)
    
    # clean vegasinsider odds teams
    vio = clean_vi('odds')
    print 'matching vi odds'
    vio_id = match_team(vio, id, min_year=2019)
    key_list.append(vio_id)
    
    # clean vegasinsider spreads teams
    visp = clean_vi('spreads')
    print 'matching vi spreads'
    visp_id = match_team(visp, id, min_year=2019)
    key_list.append(visp_id)
    
    
    # read in master id file
    key = pd.read_csv(datdir + '/scrub/teams.csv')
    key = key[['team_id', 'team_name']]

    # create universal key
    for df in key_list:
        key = pd.merge(key, df, on='team_id', how='left')
    key = key.fillna('')
    key = key.drop_duplicates()

    # set location to write file and save file
    data_out = datdir + 'interim/'
    Clean.write_file(key, data_out, 'team_key')
    
    Transfer.create_from_schema('team_key', 'data/schema.json')

    rows = Transfer.dataframe_rows(key)
    Transfer.insert('team_key', rows, at_once=True) 

def id_from_name(df, key_col, name_col, drop=True):
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
    id = Transfer.return_data('team_key')
    # from id key data, only need numeric identifer and key_col to merge on
    id = id[['team_id', key_col]]
    # remove duplicates
    id = id[~id.duplicated()]
    # join data the id key file using specified inputs
    mrg = pd.merge(df, id, left_on=name_col, right_on=key_col, how='inner')

    # list of cols to drop, key_col is redundant with name_col
    drop_cols = [key_col]
    # add name_col to drop list, if desired
    if drop == True:
        drop_cols.append(name_col)
    # remove columns from dataframe
    mrg = mrg.drop(drop_cols, axis=1)
    
    # create unique column name for added id
    id_name = name_col + '_id'
    mrg = mrg.rename(columns={'team_id': id_name})
    
    return mrg
