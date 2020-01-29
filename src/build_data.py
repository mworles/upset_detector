"""Data Builder

This script executes the full pipeline to build data used in machine 
learning models to create predictions for college basketball games. Each step in 
the pipeline is controlled by a module. This structure allows individual steps 
in the pipeline to be removed, modified, or added in a flexible manner. 

The script uses custom imports created for the project:
Constants: module containing project-wide variables
data: package used to clean and modify data
features: package used to create features
"""
# import custom modules/packages
import Constants
import data
import features

# location of data directory from constants module
datdir = Constants.DATA

# pre-process raw data
data.Clean.scrub_files(Constants.RAW_MAP)

# create files matching school names to numeric identifiers
data.Match.create_key(datdir)

# create data on wins and games for each prior tournament
data.Generate.tourney_outcomes(datdir)

# create features
features.Create.coach_features(datdir)
features.Create.team_seeds(datdir)
features.Create.team_ratings(datdir)

# merge features
features.Create.merge_features(datdir)

# combine features for both teams in matchups
data.Generate.make_matchups(datdir)

# generate targets for prediction
data.Generate.make_targets(datdir)
