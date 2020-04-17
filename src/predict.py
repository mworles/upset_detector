import pandas as pd
from models import utils
from data import clean
from constants import TEST_YEAR

# define data datdirectory, import features and targets
datdir = '../data/processed/'
df = pd.read_csv(datdir + 'features.csv', index_col=0)
targets = pd.read_csv(datdir + 'targets.csv', index_col=0)

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

# array of rows to switch for upset seed order
toswitch = clean.upset_switch(df)

# re-arrange data for predicting upsets
df = clean.upset_features(df)

# split dataset into cross-validation folds and scale data
folds_scaled = utils.split_scale(df, target, split_on, split_values)

# set up model from grid search result
grid_result = utils.get_grid_result(grid_id)
model_name = grid_result['model']
model = utils.model_set(grid_result)

# get model predictions
probs_list = utils.fold_preds(folds_scaled[0], model, type='probs', imbal=True)

# list of indices to isolate test set examples
i_test = utils.fold_split(df, 'season', TEST_YEAR)['i_test']

# isolate examples to team identifiers
test = df[df.index.isin(i_test)]
test = clean.ids_from_index(test)
test = test[['t1_team_id', 't2_team_id']]

test = clean.switch_ids(test, toswitch)

# add team names to data
test = clean.add_team_name(test, datdir='../data/')

test['t1_prob'] = probs_list
test['uprob'] = (test['t1_prob'] * 100).astype(int)

test['uprob'] = test['uprob'].astype(str).apply(lambda x: x + ' %')

test = test.drop(['t1_team_id', 't2_team_id', 't1_prob'], axis=1)
cols_rename = {'team_1': 'Underdog',
               'team_2': 'Favorite',
               'uprob': 'Upset probability'}
test = test.rename(columns=cols_rename)

test.to_html('../post/' + 'upsets_2018.html', index=False)
