import pandas as pd
from models import utils
from Cleaning import upset_features
from Constants import SPLIT_YEARS

# define data directory, import features and targets
dir = '../data/processed/'
df = pd.read_csv(dir + 'features.csv', index_col=0)
targets = pd.read_csv(dir + 'targets.csv', index_col=0)

# input variable values
split_values = SPLIT_YEARS
split_on = 'season'
score_type = 'f1'
target = targets['upset']
grid_id = 0
n_trials = 100

# remove examples missing the target
has_target = targets[targets['upset'].notnull()].index.values
df = df[df.index.isin(has_target)]
df = upset_features(df)

# split dataset into cross-validation folds and scale data
folds_scaled = utils.split_scale(df, target, split_on, split_values)

# return trials object from hyperparameter search
trials =  utils.hyper_search(grid_id, folds_scaled, n_trials, score_type)

# store the search object
utils.dump_search(grid_id, trials)

# update scores with results of search
scores =  utils.write_results(grid_id, trials)

# restore trials object from grid id
trials =  utils.get_search(0)
