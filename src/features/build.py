import pandas as pd
import coach
import roster
from src.data.transfer import DBAssist

def run():
    dba = DBAssist()

    # outcomes from previous tourneys
    df = dba.return_data('ncaa_results')
    ts = tourney.team_success(df)

    coaches = dba.return_data('coaches')

    # merge coach file with team tourney outcomes file
    # outer merge as some coaches will have no tourney games
    df = pd.merge(coaches, ts, how='outer', on=['season', 'team_id'])

    cs = coach.tourney_success(df)

    df = roster.run()

def team_success(df):
    """Uses game results to create team performance indicators for each 
    tournament year.""" 

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

def ratings_kp(datdir):
    """Create data containing team ratings."""

    def clean_season(df, season):
        """cleans inconsistent use of season. Some files contain 
        season, others Season, and others neither."""
        # if either 'Season' or 'season' in columns
        if any([c in df.columns for c in ['Season', 'season']]):
            # rename upper to lower
            df = df.rename(columns={'Season': 'season'})
        else:
            # if neither included, add column using value in season
            df['season'] = season
        # return data
        return df
    
    # location of files containing ratings for each season
    ratings_dir = datdir + '/external/kp/'
    
    # create list of file names from directory
    files = clean.list_of_files(ratings_dir)

    # use files to get lists of season numbers and dataframes
    # data has no season column so must be collected from file name and added
    seasons = [clean.year4_from_string(x) for x in files]
    dfs = [pd.read_csv(x) for x in files]

    # used nested function to create consistent season column
    data_list = [clean_season(x, y) for x, y in zip(dfs, seasons)]

    # create combined data with all seasons
    df = pd.concat(data_list, sort=False)

    # ratings data has team names, must be linked to numeric ids
    df = match.id_from_name(datdir, df, 'team_kp', 'TeamName')
    
    # for consistency
    df.columns = map(str.lower, df.columns)

    # fill missing rows due to changes in column name
    df['em'] = np.where(df['em'].isnull(), df['adjem'], df['em'])
    df['rankem'] = np.where(df['rankem'].isnull(), df['rankadjem'], df['rankem'])

    # reduce float value precision
    df = clean.round_floats(df, prec=2)

    # select columns to keep as features
    keep = ['team_id', 'season', 'adjtempo', 'adjoe', 'rankadjoe', 'adjde', 
            'rankadjde', 'em', 'rankem']
    df = df[keep]
    
    return df
