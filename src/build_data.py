"""Data Builder

This script executes the full pipeline to build data used in machine 
learning models to create predictions for college basketball games. Each step in 
the pipeline is controlled by a module. This structure allows individual steps 
in the pipeline to be removed, modified, or added in a flexible manner. 

The script uses custom imports created for the project:
constants: module containing project-wide variables
data: package used to clean and modify data
features: package used to create features
"""
# import custom modules/packages
import constants
import data

# location of data directory from constants module
datdir = constants.DATA
"""
# pre-process raw data
data.clean.scrub_files(constants.RAW_MAP)

# clean odds and spreads data
data.odds.clean_odds(datdir)
data.spreads.clean_spreads(datdir)
data.spreads.spreads_sbro(datdir)

# create files matching school names to numeric identifiers
data.match.create_key(datdir)

# create data on wins and games for each prior tournament
data.generate.tourney_outcomes(datdir)

# create features
features.Create.coach_features(datdir)
features.Create.team_seeds(datdir)
features.Create.team_ratings(datdir)

# merge features
features.Create.merge_features(datdir)


# combine features for both teams in matchups
data.generate.make_matchups(datdir)
"""
# generate targets for prediction
data.generate.make_targets(datdir)
