import pandas as pd
from models import utils
from data import Clean, Transfer
from Constants import SPLIT_YEARS

# define data datdirectory, import features and targets
#datdir = '../data/processed/'
#df = pd.read_csv(datdir + 'features.csv', index_col=0)
#targets = pd.read_csv(datdir + 'targets.csv', index_col=0)
mat = Transfer.return_data('matchups')
mat = mat.set_index('game_id')
feat_i = mat.columns.tolist().index('t1_team_off_adj')
df = mat.iloc[:, feat_i:]
target = mat['t1_marg']
# add season back in for cross-validation split
df['season'] = mat['season']

# input variable values
split_values = SPLIT_YEARS
split_on = 'season'
score_type = 'MAE'
grid_id = 2
n_trials = 200

# remove examples missing the target
#has_target = targets[targets['upset'].notnull()].index.values
#df = df[df.index.isin(has_target)]
#df = Clean.upset_features(df)

# split dataset into cross-validation folds and scale data
folds_scaled = utils.split_scale(df, target, split_on, split_values)

# return trials object from hyperparameter search
trials =  utils.hyper_search(grid_id, folds_scaled, n_trials, score_type,
                             imbal=False)

# store the trials object
utils.dump_search(grid_id, trials)

# update scores with results of search
scores = utils.write_results(grid_id, trials)
