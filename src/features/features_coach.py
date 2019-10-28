import sys
sys.path.append("../")
import pandas as pd
import numpy as np
import os
from Constants import COLUMNS_TO_RENAME
from Cleaning import write_file

print 'running %s' % (os.path.basename(__file__))

# read in data files
tteams = pd.read_csv('../../data/interim/tourney_outcomes.csv')
coach = pd.read_csv('../../data/raw/TeamCoaches.csv')

coach = coach.rename(columns=COLUMNS_TO_RENAME)
coach.columns = [x.lower() for x in coach.columns]

# merge coach file with team tourney outcomes file
coaches = pd.merge(coach, tteams, how='outer',
                   on=['season', 'team_id'])

# deal with coaches who didn't coach in tourney bc didn't finish season
# get the last day number that coach was on the team, add to coach dataframe
def my_agg(x):
    names = {'team_last': x['last_day'].max()}
    return pd.Series(names, index=['team_last'])

teamlast = coaches.groupby(['season', 'team_id']).apply(my_agg)
teamlast = teamlast.reset_index()

coaches = pd.merge(coaches, teamlast, how='outer', on=['season', 'team_id'])

# if coach wasn't still with team for tourney, set wins to missing, games to 0
coaches.ix[coaches.last_day != coaches.team_last, 'wins'] = np.NaN
coaches.ix[coaches.last_day != coaches.team_last, 'games'] = np.NaN
coaches['games'].fillna(0, inplace=True)
coaches = coaches.sort_values(['coach_name', 'season'])

# compute coach tourney experience/success features
coaches = coaches.rename(columns={'wins': 'cwins',
                                  'games': 'cgames',
                                  'coach_name': 'cname'}
                        )
# create cvis, 0/1 indicator of whether coach coached in tourney
coaches['cvis'] = np.where(coaches['cgames'] > 0, 1, 0)

# create cvisits, cumulative count of coach's prior tourneys
coaches['cvisits'] = coaches.groupby('cname')['cvis'].apply(lambda x: x.cumsum())
coaches['cvisits'] = coaches.groupby('cname')['cvisits'].apply(lambda x: x.shift(1))
coaches['cvisits'].fillna(0, inplace=True)

# create clast, indicator of whether coached was in prior year's tourney
coaches['clast'] = coaches.groupby('cname')['cvis'].apply(lambda y: y.shift(1))
coaches['clast'] = np.where(coaches['clast'].isnull(), 0, coaches['clast'])

# create cfar, the max number of rounds coach has reached
coaches['cfar'] = coaches.groupby('cname')['cgames'].apply(lambda x: x.cummax())
coaches['cfar'] = coaches.groupby('cname')['cfar'].apply(lambda y: y.shift(1))
coaches['cfar'] = np.where(coaches['cfar'].isnull(), 0, coaches['cfar'])
coaches['cwon'] = np.where(coaches['cfar'] > 1, 1, 0)

# create ce8times, number of coach's prior trips to "elite 8"(round 4)
e8dict = dict.fromkeys([0, 1, 2, 3], 0)
e8dict.update(dict.fromkeys([4, 5, 6], 1))
coaches["e8"] = coaches.cgames.map(e8dict)
coaches['ce8times'] = coaches.groupby('cname')['e8'].apply(lambda x: x.cumsum())

# create cf4times, number of coach's prior trips to "final 4"(round 5)
f4dict = dict.fromkeys([0, 1, 2, 3, 4], 0)
f4dict.update(dict.fromkeys([5, 6], 1))
coaches["f4"] = coaches.cgames.map(f4dict)
coaches['cf4times'] = coaches.groupby('cname')['f4'].apply(lambda x: x.cumsum())

# compute snkbit, indicator of "snakebit" criteria
# coach has at least 6 trips to tournament and not reached elite 8
coaches['snkbit'] = 0
coaches.loc[(coaches.cvisits >5) & (coaches.cfar < 4),'snkbit'] = 1

# keep one row per team season, keeping the coach who was coach on last day
coaches = coaches[coaches.last_day == coaches.team_last]
coaches.drop(['cvis', 'cwins', 'cgames', 'e8', 'f4', 'first_day', 'last_day',
             'team_last'], inplace=True, axis=1)

# had no pre-1985 data to compute 1985 features, so drop 1985
coaches = coaches[coaches.season > 1985]

# remove string name, not utilized as a feature
coaches = coaches.drop(['cname'], axis=1)

# save coach feature file
dest = '../../data/interim/'
file_name = 'features_coach'
write_file(coaches, dest, file_name)
