import pandas as pd
from models import utils
from Cleaning import upset_features

# define data directory, import features and targets
dir = '../data/processed/'
df = pd.read_csv(dir + 'features.csv', index_col=0)
targets = pd.read_csv(dir + 'targets.csv', index_col=0)

# input variable values
split_values = [2014, 2015, 2016, 2017, 2018]
split_on = 'season'
score_type = 'f1'
target = targets['upset']
grid_id = 0
n_trials = 100

# remove examples missing the target
has_target = targets[targets['upset'].notnull()].index.values
df = df[df.index.isin(has_target)]
df = upset_features(df)

# comment
folds_scaled = utils.split_scale(df, target, split_on, split_values)
trials =  utils.hyper_search(grid_id, folds_scaled, n_trials, score_type)
utils.dump_search(grid_id, trials)
scores =  utils.write_results(grid_id, trials)
trials =  utils.get_search(0)
