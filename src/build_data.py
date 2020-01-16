import Constants
import data
import features
#from features.features_coach import get_coach

dir = '../data/'

# pre-process raw data
data.scrub.scrub_files(Constants.RAW_MAP)

"""
# create files matching school names to numeric identifiers
ss = data.match.match_schools(dir)
kp = data.match.match_kp(dir)
data.match.combine_id(dir, [ss, kp])

# create data on wins and games for each prior tournament
data.Generate.tourney_outcomes(dir)

# create coach features
features.features_coach.get_coach(dir)

# team seeds as features
features.Create.team_seeds(dir)
"""

# merge features


#import data.make_matchups
#import data.targets
