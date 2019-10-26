import sys
sys.path.append("../")
import os
import pandas as pd
import numpy as np
from Cleaning import seed_to_numeric, write_file
from Constants import DATA_COLUMN_KEY

print 'running %s' % (os.path.basename(__file__))

# read in data files
data_in = '../../data/raw/'
tgames = pd.read_csv(data_in + 'NCAATourneyCompactResults.csv')

winner_column = DATA_COLUMN_KEY['winner_column']
loser_column = DATA_COLUMN_KEY['loser_column']

wteams = tgames[['Season', 'WTeamID']]
wteams = wteams.rename(columns={winner_column: 'team_id'})
wteams['win'] = 1

lteams = tgames[['Season', 'LTeamID']]
lteams = lteams.rename(columns={loser_column: 'team_id'})
lteams['win'] = 0

tteams = pd.concat([wteams, lteams], ignore_index=True)
tteams.columns = map(str.lower, tteams.columns)

gcols = ['season', 'team_id']

tteams = tteams.groupby(gcols)['win'].aggregate(['count', 'sum']).reset_index()
tteams = tteams.rename(columns={'count': 'games', 'sum': 'wins'})

dest = '../../data/interim/'
file_name = 'tourney_outcomes'
write_file(tteams, dest, file_name)
