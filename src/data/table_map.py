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
