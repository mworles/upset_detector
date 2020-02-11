# year to use as test set
TEST_YEAR = 2018

# minimum year to use for data inclusion, due to missing data
MIN_YEAR = 2003

# list of years to use as validation sets in chronological cross-validation
SPLIT_YEARS = [2013, 2014, 2015, 2016, 2017]

# empty dict to fill for pre-processing
# needed for consistency between data files and with existing code
RAW_MAP = {}

# each dict element has the raw file name as key
# value is a dict with 2 keys, 'new_name' and 'column'
# 'new_name' value is string for new file name
# 'columns' value is another dict with key:value pairs of strings
# key is raw column name and value is string to rename column
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

RAW_MAP['RegularSeasonCompactResults'] = {'new_name': 'reg_results',
                                          'columns': {'WTeamID': 'Wteam',
                                                      'LTeamID': 'Lteam'}
                                          }
RAW_MAP['SecondaryTourneyCompactResults'] = {'new_name': 'nit_results',
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

RAW_MAP['Seasons'] = {'new_name': 'seasons'}

# seed number to use for random data processses
RANDOM_SEED = 40195

# relative path to data directory
DATA = '../data/'
