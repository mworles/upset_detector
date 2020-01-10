import pandas as pd
import numpy as np
from models import utils, grids
import Clean
from Constants import SPLIT_YEARS, TEST_YEAR
import Plotting

# define data directory, import features and targets
dir = '../data/processed/'
df = pd.read_csv(dir + 'features.csv', index_col=0)
targets = pd.read_csv(dir + 'targets.csv', index_col=0)

# input variable values
split_values = [TEST_YEAR]
split_on = 'season'
target = targets['upset']
grid_id = 1

# remove examples missing the target
has_target = targets[targets['upset'].notnull()].index.values
df = df[df.index.isin(has_target)]

# remove examples beyond test year
df = df[df['season'] <= TEST_YEAR]

# re-arrange data for predicting upsets
df = Clean.upset_features(df)

# split dataset into cross-validation folds and scale data
folds_scaled = utils.split_scale(df, target, split_on, split_values)

# set up model from grid search result
grid_result = utils.get_grid_result(grid_id)
model_name = grid_result['model']
model = utils.model_set(grid_result)

# get model predictions
probs_list = utils.fold_preds(folds_scaled[0], model, type='probs', imbal=True)

# test example index array
i_test = utils.fold_split(df, 'season', TEST_YEAR)['i_test']

# isolate test examples
test = df[df.index.isin(i_test)]
test = Clean.ids_from_index(test)
test = test['t1_team_id', 't2_team_id']
