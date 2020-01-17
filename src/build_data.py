import Constants
import data
import features

# specify top data directory
dir = '../data/'

# pre-process raw data
data.Scrub.scrub_files(Constants.RAW_MAP)

# create files matching school names to numeric identifiers
data.Match.create_key(dir)

# create data on wins and games for each prior tournament
data.Generate.tourney_outcomes(dir)

# create features
# coach
features.Create.coach_features(dir)
# team seeds
features.Create.team_seeds(dir)
# ratings
features.Create.team_ratings(dir)

# merge features
features.Create.merge_features(dir)

# combine features for both teams in matchups
data.Generate.make_matchups(dir)

# generate targets for prediction
data.Generate.make_targets(dir)
