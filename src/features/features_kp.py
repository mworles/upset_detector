import sys
sys.path.append("../")
import os
import pandas as pd
import numpy as np
from Cleaning import list_files, add_season_column, write_file

print 'running %s' % (os.path.basename(__file__))

directory = '../../data/external/kp/'

# create list of files
files = list_files(directory, suffix=".csv")

print files
