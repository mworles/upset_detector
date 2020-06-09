# create empty dict to fill
KEY = {}

# each dict element has the raw file name as key
# value is a dict with 2 keys, 'new_name' and 'column'
# 'new_name' value is string for new file name
# 'columns' value is another dict with key:value pairs of strings
# key is raw column name and value is string to rename column
KEY['MTeamCoaches'] = {'new_name': 'coaches',
                       'columns': {'TeamID': 'team_id',
                                   'FirstDayNum': 'first_day',
                                   'LastDayNum': 'last_day',
                                   'CoachName': 'coach_name'
                                   }
                       }

KEY['MNCAATourneyCompactResults'] = {'new_name': 'ncaa_results',
                                     'columns': {'WTeamID': 'Wteam',
                                                 'LTeamID': 'Lteam'}
                                     }

KEY['MNCAATourneyDetailedResults'] = {'new_name': 'ncaa_results_dtl',
                                      'columns': {'WTeamID': 'Wteam',
                                                  'LTeamID': 'Lteam'}
                                      }

KEY['MTeams'] = {'new_name': 'teams',
                 'columns': {'TeamID': 'team_id',
                             'TeamName': 'team_name'}
                 }

KEY['MTeamSpellings'] = {'new_name': 'team_spellings',
                         'columns': {'TeamID': 'team_id',
                                     'TeamNameSpelling': 'name_spelling'}
                         }

KEY['MRegularSeasonCompactResults'] = {'new_name': 'reg_results',
                                       'columns': {'WTeamID': 'Wteam',
                                                   'LTeamID': 'Lteam'}
                                       }

KEY['MRegularSeasonDetailedResults'] = {'new_name': 'reg_results_dtl',
                                        'columns': {'WTeamID': 'Wteam',
                                                    'LTeamID': 'Lteam'}
                                        }

KEY['MNCAATourneySeeds'] = {'new_name': 'seeds',
                            'columns': {'TeamID': 'team_id'}
                            }

KEY['MSeasons'] = {'new_name': 'seasons'}

KEY['MGameCities'] = {'new_name': 'game_cities',
                      'columns': {'WTeamID': 'WTeam',
                                  'LTeamID': 'LTeam',
                                  'CRType': 'game_cat',
                                  'CityID': 'city_id'}
                               }

KEY['Cities'] = {'new_name': 'cities',
                 'columns': { 'CityID': 'city_id'}
                 }

KEY['TeamGeog'] = {'new_name': 'team_geog',
                   'columns': { 'lat': 'latitude',
                               'lng': 'longitude'}
                   }

KEY['TourneyGeog'] = {'new_name': 'tourney_geog',
                      'columns': {'lat': 'latitude',
                                  'lng': 'longitude'}
                      }
