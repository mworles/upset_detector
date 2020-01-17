"""Data Builder

This script executes the full pipeline to build the data used in machine 
learning models to create predictions for college basketball games. Each step in 
the pipeline is controlled by a module. This is for comprehension and to allow 
flexibility in pipeline control, as each step can easily be removed, modified, or added.

The script uses custom imports created for the project:
Constants: module containing project-wide variables
data: package containing modules used to clean and generate interim data
features: package with modules used to create features
"""

import Constants
import data
import features

# location of data
dir = Constants.DATA

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
