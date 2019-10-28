import sys
sys.path.append("../")
import pandas as pd
from Cleaning import write_file
from Constants import COLUMNS_TO_RENAME
import os

print 'running %s' % (os.path.basename(__file__))

# read in team seeds file to get dataset of unique team-seasons
dir = '../../data/'
ts = pd.read_csv(dir + '/raw/NCAATourneySeeds.csv')

# minor cleaning
ts = ts.rename(columns=COLUMNS_TO_RENAME)
ts.columns = ts.columns.str.lower()
ts = ts.drop('seed', axis=1)

# import coach features, merge with team seasons
f_coach = pd.read_csv(dir + 'interim/features_coach.csv')
f = pd.merge(ts, f_coach, how='inner', on=['team_id', 'season'])

# import kp features, merge with other features
f_kp = pd.read_csv(dir + 'interim/features_kp.csv')
f = pd.merge(f, f_kp, how='inner', on=['team_id', 'season'])

# save features data
write_file(f, '../../data/interim/', 'features_all')
