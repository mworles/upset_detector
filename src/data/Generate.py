"""Get yearly team tourney outcomes.

This script uses data on prior year tournament results to create a dataset of
the number of tournament games and wins for each team for each year. This data
is saved for use by other scripts to compute various team features. 

This script requires `pandas`. It imports the custom Clean module.

"""

import pandas as pd
import Clean

def tourney_outcomes(dir):
    # read in prior tournament results
    data_in = dir + 'scrub/'
    tgames = pd.read_csv(data_in + 'ncaa_results.csv')

    # data is one row per game
    # separate winners and losers, to create a team-specific win indicator
    # winners
    wteams = tgames[['season', 'wteam']]
    wteams = wteams.rename(columns={'wteam': 'team_id'})
    wteams['win'] = 1
    
    # losers
    lteams = tgames[['season', 'lteam']]
    lteams = lteams.rename(columns={'lteam': 'team_id'})
    lteams['win'] = 0

    # combine data to create one row per team per game
    df = pd.concat([wteams, lteams], ignore_index=True)

    # columns to group by
    gcols = ['season', 'team_id']

    # count and sum number of rows per "group"
    df = df.groupby(gcols)['win'].aggregate(['count', 'sum']).reset_index()

    # count is the number of games, sum is the number of wins
    df = df.rename(columns={'count': 'games', 'sum': 'wins'})
    
    # write file
    Clean.write_file(df, dir + 'interim/', 'tourney_outcomes')
