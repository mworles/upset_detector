import sys
sys.path.append("../")
import os
import pandas as pd
import numpy as np
from Cleaning import list_files, get_season, add_season, write_file

print 'running %s' % (os.path.basename(__file__))

directory = '../../data/external/kp/'

# create list of file names
files = [directory + x for x in list_files(directory, suffix=".csv")]

# use files to get lists of season numbers and dataframes
seasons = [get_season(x) for x in files]
dfs = [pd.read_csv(x) for x in files]

# add season column
data_list = [add_season(x, y) for x, y  in zip(dfs, seasons)]

df = pd.concat(data_list, sort=False)

# link team id numbers
kpid = pd.read_csv('../../data/interim/kp_ids.csv')
mrg = pd.merge(df, kpid, on='TeamName', how='inner')

# remove columns not needed
mrg.columns = map(str.lower, mrg.columns)

# fill missing rows due to changes in column name
mrg['em'] = np.where(mrg['em'].isnull(), mrg['adjem'], mrg['em'])
mrg['rankem'] = np.where(mrg['rankem'].isnull(), mrg['rankadjem'], mrg['rankem'])

# select columns to keep as features
keep = ['team_id', 'season', 'adjtempo', 'adjoe', 'rankadjoe',
              'adjde', 'rankadjde', 'em', 'rankem']
mrg = mrg[keep]

# save kp feature data file
write_file(mrg, '../../data/interim/', 'features_kp')
