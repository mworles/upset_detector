from data import Transfer
from data import Updater
from data import queries
from data import Generate
import pandas as pd
import numpy as np
import pickle
import Constants
from models import utils
"""
def make_features(df, tables):
    if 'ratings_needed' in tables:
        ratings = Transfer.return_data('ratings_needed')
        ratings = ratings.drop('season', axis=1)
        df = Updater.assign_features(df, ratings, merge_on=['date'])
    
    if 'team_home' in tables:
        home = Transfer.return_data('team_home')
        home = home.drop('game_id', axis=1)
        df = Updater.assign_features(df, home, merge_on=['date'])
    
    if 'spread' in tables:
        spread = Transfer.return_data('spreads_by_team')
        spread = spread.drop('game_id', axis=1)
        spread['spread'] = - spread['spread']
        spread = spread.rename(columns={'spread': 'spread_rev'})
        df = Updater.assign_features(df, spread, team='t1', merge_on=['date'])
    return df

def add_result(df, target):
    results = Transfer.return_data('results_by_team')
    game_cols = ['game_id', 'team_id', 'date']
    target_drop = [x for x in results.columns if x not in game_cols]
    target_drop.remove(target)
    results = results.drop(['game_id'], axis=1)
    results = results.drop(target_drop, axis=1)
    results = results.rename(columns={'team_id': 't1_team_id'})
    df = pd.merge(df, results, left_on=['t1_team_id', 'date'],
                  right_on=['t1_team_id', 'date'], how='inner')
    return df

def add_spreads(df):
    # merge spreads
    spreads = Transfer.return_data('spreads_by_team')
    spreads = spreads.rename(columns={'team_id': 't1_team_id'})
    spreads = spreads.drop(['game_id'], axis=1)
    df = pd.merge(df, spreads, how='left',
                  left_on=['t1_team_id', 'date'],
                  right_on=['t1_team_id', 'date'])
    return df

def add_target(df, target):
    if target == 'ats':
        df = add_result(df, 'marg')
        df = add_spreads(df)
        spread_rev = - df['spread']
        df['target'] = (df['marg'] >= spread_rev).astype(int)
        df = df.drop(['marg', 'spread'], axis=1)
    if target == 'win':
        df = add_result(df, 'win')
        df = df.rename(columns={'win': 'target'})
    df = df[df['target'].notnull()]
    return df


def clean_set(df):
    df = df.drop_duplicates()
    df = df.set_index('game_id')
    df = df.drop(['date', 't1_team_id', 't2_team_id'], axis=1)
    df = df.dropna(how='any')
    return df

def get_examples(arrange='neutral'):
    if arrange == 'neutral':
        df = Transfer.return_data('game_info')
    else:
        df = Transfer.return_data('fav_dog')
        if arrange == 'favorite':
            col_map = {'t_favor': 't1_team_id', 't_under': 't2_team_id'}
        elif arrange == 'underdog':
            col_map = {'t_under': 't1_team_id', 't_favor': 't2_team_id'}
        else:
            exit()
        df = df.rename(columns=col_map)
    df = df[['game_id', 'season', 'date', 't1_team_id', 't2_team_id']]
    return df

# import matchups with ordered favorites and underdogs
df = get_examples(arrange='underdog')
tables = ['ratings_needed', 'team_home', 'spread']
df = make_features(df, tables)
df = add_target(df, 'win')
df = clean_set(df)
df.to_pickle('df2.pkl')
"""
df = pd.read_pickle('df2.pkl')
df = df.reset_index()
df = df.drop_duplicates(subset=['game_id'])
df = df.set_index('game_id')

split_on = 'season'
split_values = Constants.SPLIT_YEARS
folds_scaled = utils.split_scale(df, split_on, split_values)

score_type = 'f1'
n_trials = 100
grid_id = 5
exp_id = 8

trials =  utils.hyper_search(grid_id, folds_scaled, n_trials, score_type,
                             imbal=True)
# store the trials object
utils.dump_search(grid_id, trials)
# update scores with results of search
scores = utils.write_results(exp_id, grid_id, trials)




"""
from models.grids import scorer_grid

class SearchExp():

    def __init__(self, id, trials, features, targets):
        self.id = id
        self.trials = trials
        self.features = features
        self.targets = targets
        self.grid_id = grid_id

    def use_score(self, score):
        self.score_type = score
        self.scorer = scorer_grid[self.score_type]['function']
        
    def split_scale(self, split_on, split_values):
        

ratings = pd.DataFrame([[1, 2], [1, 2]], index=[0, 1], columns=['col0', 'col1'])
targets = pd.Series([0, 1], index=[0, 1])
grid_id = 5

exp = SearchExp(1, 100, ratings, targets, grid_id)
exp.use_score('f1')
exp.split_scale()
"""
