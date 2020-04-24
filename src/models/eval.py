import pandas as pd
import numpy as np
from models import utils, grids
from data import clean
from constants import SPLIT_YEARS
import plot

# define data datdirectory, import features and targets
datdir = '../data/processed/'
df = pd.read_csv(datdir + 'features.csv', index_col=0)
targets = pd.read_csv(datdir + 'targets.csv', index_col=0)

# input variable values
split_values = SPLIT_YEARS
split_on = 'season'
score_type = 'f1'
target = targets['upset']
grid_id = 1 

# remove examples missing the target
has_target = targets[targets['upset'].notnull()].index.values
df = df[df.index.isin(has_target)]
df = clean.upset_features(df)

# split dataset into cross-validation folds and scale data
folds_scaled = utils.split_scale(df, target, split_on, split_values)

grid_result = utils.get_grid_result(grid_id)
model_name = grid_result['model']

model = utils.model_set(grid_result)

probs_list = map(lambda x: utils.fold_preds(x, model, type='probs', imbal=True),
                 folds_scaled)
y_probs = np.concatenate(probs_list).ravel().tolist()

preds_list = map(lambda x: utils.fold_preds(x, model, type='labels', imbal=True),
                 folds_scaled)
y_preds = np.concatenate(preds_list).ravel().tolist()

labels_list = [x['y_test'] for x in folds_scaled]
y = [i for sub in labels_list for i in sub]

plot.plot_roc_curve(y, y_probs, grid_id)
plot.plot_confusion_matrix(y, y_preds, grid_id)
