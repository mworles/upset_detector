# year to use as test set
TEST_YEAR = 2020

# minimum year to use for data inclusion, due to missing data
MIN_YEAR = 2003

# list of years to use as validation sets in chronological cross-validation
SPLIT_YEARS = [2015, 2016, 2017, 2018, 2019]

# empty dict to fill for pre-processing
# needed for consistency between data files and with existing code
RAW_MAP = {}

# each dict element has the raw file name as key
# value is a dict with 2 keys, 'new_name' and 'column'
# 'new_name' value is string for new file name
# 'columns' value is another dict with key:value pairs of strings
# key is raw column name and value is string to rename column
RAW_MAP['MTeams'] = {'new_name': 'teams',
                     'columns': {'TeamID': 'team_id',
                                 'TeamName': 'team_name'}
                      }

RAW_MAP['MTeamSpellings'] = {'new_name': 'team_spellings',
                            'columns': {'TeamID': 'team_id',
                                        'TeamNameSpelling': 'name_spelling'}
                            }

RAW_MAP['MNCAATourneyCompactResults'] = {'new_name': 'ncaa_results',
                                        'columns': {'WTeamID': 'Wteam',
                                                    'LTeamID': 'Lteam'}
                                        }

RAW_MAP['MRegularSeasonCompactResults'] = {'new_name': 'reg_results',
                                          'columns': {'WTeamID': 'Wteam',
                                                      'LTeamID': 'Lteam'}
                                          }
RAW_MAP['MSecondaryTourneyCompactResults'] = {'new_name': 'nit_results',
                                             'columns': {'WTeamID': 'Wteam',
                                                         'LTeamID': 'Lteam'}
                                             }

RAW_MAP['MNCAATourneyDetailedResults'] = {'new_name': 'ncaa_results_dtl',
                                        'columns': {'WTeamID': 'Wteam',
                                                    'LTeamID': 'Lteam'}
                                        }

RAW_MAP['MRegularSeasonDetailedResults'] = {'new_name': 'reg_results_dtl',
                                          'columns': {'WTeamID': 'Wteam',
                                                      'LTeamID': 'Lteam'}
                                          }

RAW_MAP['MTeamCoaches'] = {'new_name': 'coaches',
                          'columns': {'TeamID': 'team_id',
                                      'FirstDayNum': 'first_day',
                                      'LastDayNum': 'last_day',
                                      'CoachName': 'coach_name',
                                      }
                          }

RAW_MAP['MNCAATourneySeeds'] = {'new_name': 'seeds',
                               'columns': {'TeamID': 'team_id'}
                               }

RAW_MAP['MSeasons'] = {'new_name': 'seasons'}

RAW_MAP['MGameCities'] = {'new_name': 'game_cities',
                          'columns': {'WTeamID': 'WTeam',
                                      'LTeamID': 'LTeam',
                                      'CRType': 'game_cat',
                                      'CityID': 'city_id'}
                               }

RAW_MAP['Cities'] = {'new_name': 'cities',
                     'columns': { 'CityID': 'city_id'}
                     }

# seed number to use for random data processses
RANDOM_SEED = 40195

# relative path to data directory
DATA = '../data/'
