import pandas as pd
import coach
import roster
from src.data.transfer import DBAssist

def run():
    coach_features = coach.run()
    

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
    
    def year4_from_string(s):
        """Returns numeric 4-digit year from string containing 2-digit year."""
        # extract digits from string
        year2 = "".join(re.findall('\d+', s))
        # default century is 2000
        pre = '20'
        # if final 2 year digits 80 or more, change prefix to 19
        if int(year2) > 80:
            pre = '19'
        # create 4-digit numeric year
        year4 = int(''.join([pre, year2]))
        return year4    

    def round_floats(df, prec=2):
        """Returns dataframe with all float values rounded to specified precision. 
        
        Arguments
        ----------
        df: pandas dataframe
            Contains float numeric desired to be rounded. 
        prec: integer
            The desired decimal point precision for rounding. Default is 2. 
        """
        for c in df.columns:
            # only round columns of float data type
            if df[c].dtype == 'float':
                df[c] = df[c].round(decimals=prec)
        return df
    
    # location of files containing ratings for each season
    ratings_dir = datdir + '/external/kp/'
    
    # create list of file names from directory
    files = clean.list_of_files(ratings_dir)

    # use files to get lists of season numbers and dataframes
    # data has no season column so must be collected from file name and added
    seasons = [year4_from_string(x) for x in files]
    dfs = [pd.read_csv(x) for x in files]

    # used nested function to create consistent season column
    data_list = [clean_season(x, y) for x, y in zip(dfs, seasons)]

    # create combined data with all seasons
    df = pd.concat(data_list, sort=False)

    # ratings data has team names, must be linked to numeric ids
    df['team_id'] = match.ids_from_names(df['TeamName'].values, 'team_kp')

    # for consistency
    df.columns = map(str.lower, df.columns)

    # fill missing rows due to changes in column name
    df['em'] = np.where(df['em'].isnull(), df['adjem'], df['em'])
    df['rankem'] = np.where(df['rankem'].isnull(), df['rankadjem'], df['rankem'])

    # reduce float value precision
    df = round_floats(df, prec=2)

    # select columns to keep as features
    keep = ['team_id', 'season', 'adjtempo', 'adjoe', 'rankadjoe', 'adjde', 
            'rankadjde', 'em', 'rankem']
    df = df[keep]
    
    return df
