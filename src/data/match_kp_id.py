import sys
sys.path.append("../")
import os
import pandas as pd
import numpy as np
import re
from fuzzywuzzy import process
from Cleaning import list_files, write_file
from Constants import COLUMNS_TO_RENAME

print "running %s" % (os.path.basename(__file__))

kpath = '../../data/external/kp/'
dpath = '../../data/raw/'

# create list of files
files = [directory + x for x in list_files(directory, suffix=".csv")]

# list of dataframes, add season column
data_list = [pd.read_csv(x) for x in files]

# combine into single data frame
df = pd.concat(data_list, sort=True)

# modify kp team names to match kaggle format and improve matching
df['team_kp'] = df['TeamName'].str.lower()
df['team_kp'] = df['team_kp'].str.replace('southwest texas st.', 'texas st.')

# make dataframe of the kp names
dfkp = pd.DataFrame(pd.unique(df['team_kp']), columns=['team_kp'])

# read in kaggle team id number and names file
dftn = pd.read_csv(dpath + '/TeamSpellings.csv')
dftn = dftn.rename(columns=COLUMNS_TO_RENAME)

# create a dictionary of team_ids by name_spelling to use as index
dfts = dftn.set_index('name_spelling')
dftsd = dfts.to_dict('index')

# attempt to merge the kaggle and kp team names
mrg = pd.merge(dfkp, dftn, how='outer',
				left_on='team_kp', right_on='name_spelling',
				indicator=True)
match = mrg[mrg['_merge'] == 'both']
match = match.iloc[:, 0:-1]

# pull dataframe of nonmatched kp names
nomatch = mrg[mrg['_merge'] == "left_only"]

# get the nonmatched kaggle team names
kaggle_not = mrg[mrg['_merge'] == 'right_only']
nomat_names = kaggle_not['name_spelling'].tolist()

# create a function to fuzzy string match using nonmatched kaggle names
def match_team(row):
    team_name = row['team_kp']
    result = list(process.extractOne(team_name, nomat_names))
    match_list = [team_name] + result
    team_id = dftsd[result[0]]['team_id']
    match_list.insert(2, team_id)
    return match_list

# apply the function to nonmatched kp names
team_matches = nomatch.apply(match_team, axis=1)
team_matches = [x[:-1] for x in team_matches]
dfnm = pd.DataFrame(team_matches, columns=['team_kp',
                                           'name_spelling',
                                           'team_id'])

# get a datframe of the kp name - kaggle identifer pairings
kp_ids = pd.concat([match, dfnm])

kp_ids['team_id'] = kp_ids['team_id'].astype(int)

# merge the kp data file with the kaggle identifiers
mrg2 = pd.merge(df, kp_ids, on='team_kp', how='inner')
cols_to_keep = ['TeamName', 'name_spelling', 'team_id']
mrg2 = mrg2[cols_to_keep]
mrg2.drop_duplicates(inplace=True)

dest = '../../data/interim/'
file_name = 'id_kp'
write_file(mrg2, dest, file_name)
