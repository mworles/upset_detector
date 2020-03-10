def ratings_kp(datdir):
    """Create data containing team ratings."""

    def clean_season(df, season):
        """Cleans inconsistent use of season. Some files contain 
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
    files = Clean.list_of_files(ratings_dir)

    # use files to get lists of season numbers and dataframes
    # data has no season column so must be collected from file name and added
    seasons = [Clean.year4_from_string(x) for x in files]
    dfs = [pd.read_csv(x) for x in files]

    # used nested function to create consistent season column
    data_list = [clean_season(x, y) for x, y in zip(dfs, seasons)]

    # create combined data with all seasons
    df = pd.concat(data_list, sort=False)

    # ratings data has team names, must be linked to numeric ids
    df = Match.id_from_name(datdir, df, 'team_kp', 'TeamName')
    
    # for consistency
    df.columns = map(str.lower, df.columns)

    # fill missing rows due to changes in column name
    df['em'] = np.where(df['em'].isnull(), df['adjem'], df['em'])
    df['rankem'] = np.where(df['rankem'].isnull(), df['rankadjem'], df['rankem'])

    # reduce float value precision
    df = Clean.round_floats(df, prec=2)

    # select columns to keep as features
    keep = ['team_id', 'season', 'adjtempo', 'adjoe', 'rankadjoe', 'adjde', 
            'rankadjde', 'em', 'rankem']
    df = df[keep]

    # save team ratings file
    data_out = datdir + 'features/'
    Clean.write_file(df, data_out, 'team_ratings')
