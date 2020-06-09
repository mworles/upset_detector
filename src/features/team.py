import pandas as pd
import numpy as np
from src.data.transfer import DBAssist


def tourney_performance(modifier=None):
    """Uses game results to create team performance indicators for each 
    tournament year.""" 
    dba = DBAssist()
    # import results from previous tournaments
    df = dba.return_data('ncaa_results', modifier=modifier)
    dba.close()

    # separate winners and losers, to create a team-specific win indicator
    # winners
    wteams = df[['season', 'wteam']]
    wteams = wteams.rename(columns={'wteam': 'team_id'})
    wteams['win'] = 1
    
    # losers
    lteams = df[['season', 'lteam']]
    lteams = lteams.rename(columns={'lteam': 'team_id'})
    lteams['win'] = 0

    # combine data to create one row per team per game
    by_team = pd.concat([wteams, lteams], ignore_index=True)

    # columns to group by
    gcols = ['season', 'team_id']
    
    # count and sum number of rows per "group"
    by_team = by_team.groupby(gcols)['win'].aggregate(['count', 'sum']).reset_index()

    # count is the number of games, sum is the number of wins
    by_team = by_team.rename(columns={'count': 'games',
                                      'sum': 'wins'})
    
    
    
    
    return by_team
