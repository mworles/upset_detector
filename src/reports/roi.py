import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from imblearn.over_sampling import RandomOverSampler
from imblearn.pipeline import make_pipeline
from sklearn.metrics import f1_score
from src.data.transfer import DBAssist

data_path = '../../data/processed/'

# load the training set features and targets
train = pd.read_csv(data_path + 'xtrain.csv')
train_targets = pd.read_csv(data_path + 'ytrain.csv', header=None).values
train_features = train.drop(['season', 'rnd'], axis=1)

# scale training set
sc = StandardScaler()
sc.fit(train_features)
train_features_scaled = sc.transform(train_features)

# import 'test set' of 2017 matchups, keep games with seed differential >=4
test = pd.read_csv('../../data/processed/matchups_2017.csv')
test = test[test['upsetpot'] == 1]

# get the round, seeds, and teams
test_matchups = test[['rnd', 't1_team_id', 't2_team_id', 't1_seed',
                        't2_seed', 'upset']].copy()

# get array of targets
test_targets = test['upset'].values

# select, order, and scale test set features
test_features = test.drop(['season', 'daynum', 'upset', 'upsetpot', 'win_t2', 't1_team_id',
                  't2_team_id', 'Win', 'rnd'], axis=1)
test_features = test_features.rename(columns={'dstdelt': 'dist_delt'})
test_features = test_features[train_features.columns]
test_features_scaled = sc.transform(test_features)


clf = LogisticRegression(random_state=0, solver='liblinear', C=0.066381,
                         penalty='l1')
sampler = RandomOverSampler(random_state=0, sampling_strategy=1.0)
pipe = make_pipeline(sampler, clf)
pipe.fit(train_features_scaled, train_targets)
test_predictions = pipe.predict(test_features_scaled)
test_probabilities = pipe.predict_proba(test_features_scaled)[:, 1]

# add predictions to matchups
test_matchups['prediction'] = test_predictions
test_matchups['probability'] = test_probabilities


team_odds = pd.read_csv(data_path + 'odds_2017.csv')

# merge with game data
test_odds = pd.merge(test_matchups, team_odds, left_on=['t1_team_id', 'rnd'],
                     right_on=['team_id', 'rnd'], how='inner')
test_odds = test_odds.rename(columns={'monlin': 't1_odds'})
test_odds = test_odds.drop('team_id', axis=1)

test_odds = pd.merge(test_odds, team_odds, left_on=['t2_team_id', 'rnd'],
                     right_on=['team_id', 'rnd'], how='inner')
test_odds = test_odds.rename(columns={'monlin': 't2_odds'})
test_odds = test_odds.drop('team_id', axis=1)

# set wager amount
test_odds['bet_amount'] = 100

# get odds of the predicted winner
prediction_odds = test_odds[['prediction', 't1_odds', 't2_odds']].values

def odds_of_prediction(row):
    if row[0] == 1:
        return row[2]
    else:
        return row[1]

def decimal_odds(odds):
    if odds < 0:
        decimal = (100/abs(odds)) + 1
    else:
        decimal =  (odds/100) +1
    return round(decimal, 3)    

    

prediction_odds = list(map(odds_of_prediction, prediction_odds))
test_odds['odds_decimal'] = list(map(decimal_odds, prediction_odds))
test_odds['bet_return'] = test_odds['bet_amount'] * test_odds['odds_decimal']
prediction_correct = test_odds['upset'] == test_odds['prediction']
test_odds['bet_won'] = np.where(prediction_correct, 1, 0)
test_odds['amount_won'] = test_odds['bet_won'] * test_odds['bet_return']
test_odds['net_won'] = (test_odds['amount_won'] - test_odds['bet_amount']).round(2)


def add_team_names(game_data):
    dba = DBAssist()
    teams = dba.return_data('teams', subset=['team_id', 'team_name'])
    dba.close()
    
    new_data = pd.merge(game_data, teams, left_on=['t1_team_id'],
                        right_on='team_id')
    new_data = new_data.rename(columns={'team_name': 'Favorite'})
    new_data = new_data.drop('team_id', axis=1)

    new_data = pd.merge(new_data, teams, left_on=['t2_team_id'],
                        right_on='team_id')
    new_data = new_data.rename(columns={'team_name': 'Underdog'})
    new_data = new_data.drop('team_id', axis=1)
    
    return new_data

test_odds = add_team_names(test_odds)
test_odds = test_odds.sort_values(['rnd'])
test_odds['net_cumulative'] = test_odds['net_won'].cumsum()

test_odds.to_csv(data_path + '../results/bets_2017.csv', index=False)
