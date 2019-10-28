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

# combine into single data frame
df = pd.concat(data_list, sort=True)
