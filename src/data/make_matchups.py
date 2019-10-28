import sys
sys.path.append("../")
import pandas as pd
import os
from Cleaning import write_file, set_gameid_index, convert_team_id

print 'running %s' % (os.path.basename(__file__))

# read in data file of past game results
dir = '../../data/raw/'
df = pd.read_csv(dir + 'NCAATourneyCompactResults.csv')

# minor cleaning
df.columns = df.columns.str.lower()

# identify team id columns
team_cols = [x for x in df.columns if 'team' in x]
df = set_gameid_index(df, team_cols)
df = convert_team_id(df, team_cols)


print df.head()
'''
df.columns = map(lambda x: x.lower(), df.columns)
team_cols = [x for x in df.columns if 'team' in x]
df = convert_team_id(df, team_cols)
'''
