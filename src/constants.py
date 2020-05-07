import os

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

RAW_MAP['TeamGeog'] = {'new_name': 'team_geog',
                       'columns': { 'lat': 'latitude',
                                   'lng': 'longitude'}
                       }
RAW_MAP['TourneyGeog'] = {'new_name': 'tourney_geog',
                          'columns': {'lat': 'latitude',
                                      'lng': 'longitude'}
                       }

# seed number to use for random data processses
RANDOM_SEED = 40195

# create absolute path to this file
PATH_HERE = os.path.abspath(os.path.dirname(__file__))

# relative path to data directory
DATA = '../data/'
DATA_DIR = os.path.join(PATH_HERE, DATA)

DB_NAME = 'bball'

CONFIG = '../.config'
CONFIG_FILE= os.path.join(PATH_HERE, CONFIG)

SCHEMA = 'data/schema.json'
SCHEMA_FILE = os.path.join(PATH_HERE, SCHEMA)

TEAM_CITY_UPDATE =  [[1465, ('Riverside', 'California')],
                     [1466, ('Florence', 'Alabama')]
                     ]

SOURCE_ID_YEARS = {'team_ss': 1993,
                   'team_kp': 2002,
                   'team_pt': 2003,
                   'team_oddsport': 2009,
                   'team_tcp': 2019,
                   'team_vi_odds': 2019,
                   'team_vi_spreads': 2019,
                   'team_sbro': 2009}
