COLUMNS_TO_RENAME = {'TeamID': 'team_id',
                     'LastDayNum': 'last_day',
                     'CoachName': 'coach_name',
                     'FirstDayNum': 'first_day',
                     'TeamNameSpelling': 'name_spelling',
                     'ConfAbbrev': 'conference',
                     'WTeamID': 'Wteam',
                     'LTeamID': 'Lteam',
                     'WLoc': 'Wloc'}

DATA_COLUMN_KEY = {'winner_column': 'WTeamID',
                   'loser_column': 'LTeamID'}

TEST_YEAR = 2018

MIN_YEAR = 2003

SPLIT_YEARS = [2013, 2014, 2015, 2016, 2017]

RAW_MAP = {'Teams': {'new_name': 'teams',
                     'cols_rename': {'TeamID': 'team_id',
                                     'TeamName': 'team_name'}
                      }
           }

RANDOM_SEED = 40195
