"""Get yearly team tourney outcomes.

This script uses data on prior year tournament results to create a dataset of
the number of tournament games and wins for each team for each year. This data
is saved for use by other scripts to compute various team features. 

This script requires `pandas`. It imports the custom Clean module.

"""

import pandas as pd
import Clean

# read in prior tournament results data
data_in = '../data/scrub/'
tgames = pd.read_csv(data_in + 'ncaa_results.csv')

# data is one row per game
# separate winners and losers create a team-specific win indicator to count
wteams = tgames[['season', 'wteam']]
wteams = wteams.rename(columns={'wteam': 'team_id'})
wteams['win'] = 1

lteams = tgames[['season', 'lteam']]
lteams = lteams.rename(columns={'lteam': 'team_id'})
lteams['win'] = 0

# combine data to create one row per team per game
tteams = pd.concat([wteams, lteams], ignore_index=True)

# columns to group by
gcols = ['season', 'team_id']

# count and sum number of rows per "group"
tteams = tteams.groupby(gcols)['win'].aggregate(['count', 'sum']).reset_index()

# count is the number of games, sum is the number of wins
tteams = tteams.rename(columns={'count': 'games', 'sum': 'wins'})

# save data
data_out = '../data/interim/'
file_name = 'tourney_outcomes'
Clean.write_file(tteams, data_out, file_name)
