COLUMNS_TO_RENAME = {'TeamID': 'team_id',
                     'LastDayNum': 'last_day',
                     'CoachName': 'coach_name',
                     'FirstDayNum': 'first_day',
                     'TeamNameSpelling': 'name_spelling',
                     'ConfAbbrev': 'conference',
                     'WTeamID': 'Wteam',
                     'LTeamID': 'Lteam',
                     'WLoc': 'Wloc'}

TEST_YEAR = 2018

MIN_YEAR = 2003

SPLIT_YEARS = [2013, 2014, 2015, 2016, 2017]

RAW_MAP = {}

RAW_MAP['Teams'] = {'new_name': 'teams',
                     'columns': {'TeamID': 'team_id',
                                 'TeamName': 'team_name'}
                      }
RAW_MAP['TeamSpellings'] = {'new_name': 'team_spellings',
                            'columns': {'TeamID': 'team_id',
                                        'TeamNameSpelling': 'name_spelling'}
                            }
RAW_MAP['NCAATourneyCompactResults'] = {'new_name': 'ncaa_results',
                                        'columns': {'WTeamID': 'Wteam',
                                                    'LTeamID': 'Lteam'}
                                        }
RAW_MAP['TeamCoaches'] = {'new_name': 'coaches',
                          'columns': {'TeamID': 'team_id',
                                      'FirstDayNum': 'first_day',
                                      'LastDayNum': 'last_day',
                                      'CoachName': 'coach_name',
                                      }
                          }

RAW_MAP['NCAATourneySeeds'] = {'new_name': 'seeds',
                               'columns': {'TeamID': 'team_id'}
                               }

RANDOM_SEED = 40195

DATA = '../data/'
