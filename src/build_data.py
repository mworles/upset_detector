import Constants
import data
from features.features_coach import get_coach
dir = '../data/'

# first cleaning step to change variable names and case for consistency
#data.scrub.scrub_files(Constants.RAW_MAP)
"""
# create files matching school names to numeric identifiers
ss = data.match.match_schools(dir)
kp = data.match.match_kp(dir)
data.match.combine_id(dir, [ss, kp])
"""
# create data on wins and games for each prior tournament
from data import get_tourney_outcomes

data.Clean.write_file(get_coach(), dir + 'features/', 'features_coach')
#import data.make_matchups
#import data.targets
