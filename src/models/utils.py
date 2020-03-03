"""Modeling utility tools.

This module contains functions used for modeling data. Functions are here to
isolate and package code used to create cross-validation splits, scale data, 
create cross-validation predictions, and perform hyperparameter searches. 

This script requires `pandas`, `numpy`, and `fuzzywuzzy` packages. 
It uses base Python packages `os`, `re`, and `datetime`. 

"""

import pandas as pd
import numpy as np
import json
import pickle
from sklearn.preprocessing import StandardScaler
from hyperopt import fmin, tpe, Trials, space_eval, hp, STATUS_OK
from grids import model_grid, space_grid, scorer_grid
from sklearn.linear_model import LogisticRegression, Ridge
#from imblearn.over_sampling import RandomOverSampler
from Constants import RANDOM_SEED

def fold_split(df, split_on, split_value):
    """Returns a dictionary containing indices to identify training set and
    test set examples from the input dataframe. To perform sequential 
    cross-validation, the training and test set are identified using specified
    criteria in the data instead of randomly.
    
    Arguments
    ----------
    df: pandas dataframe 
        Contains a column with values to use to split the data into training
        and test set.
    split_on: string
        The column to use to split.
    split_value: numeric
        The value that identifies example to use as test set.
    """
    # index values for training set 
    # for training, want examples that occur before split_value
    train = df[df[split_on] < split_value].index.values
    # test set is examples equal to split_value
    test = df[df[split_on] == split_value].index.values
    
    # return dict
    dict_split = {'fold': split_value, # 'fold': split_value as identifier
                  'i_train': train, # i_train for list of train examples
                  'i_test': test} # i_test for list of test examples
    return dict_split

def custom_folds(df, split_on, split_values):
    """Split data into cross_validation folds using values of some array. 
    
    Creates n pairs of training and validation sets n is length of split_values.
    
    Args:
        split_on: column to use to split
        split_values: list of values that define each validation fold
    """
    x = map(lambda x: fold_split(df, split_on, x), split_values)
    return x

def fit_scaler(df, split_on, split_values, drop=True):
    # fit data scaler to training examples
    fold_min = min(split_values)
    scale_data = df[df[split_on] < fold_min]
    if drop:
        scale_data = scale_data.drop(split_on, axis=1)
    sc = StandardScaler()
    scaler = sc.fit(scale_data.astype(float))
    return scaler

def scale_fold(df, target, fold, scaler):
    xt = df[df.index.isin(fold['i_train'])].values
    xv = df[df.index.isin(fold['i_test'])].values

    yt = target.loc[fold['i_train']].values
    yv = target.loc[fold['i_test']].values

    xts = scaler.transform(xt.astype(float))
    xvs = scaler.transform(xv.astype(float))
    
    fd = {'fold': fold['fold'],
          'x_train': xts,
          'x_test': xvs,
          'y_train': yt,
          'y_test': yv}
    fold.update(fd)
    return fold

def scale_folds(df, target, folds, scaler):
    tf = lambda x: scale_fold(df, target, x, scaler)
    folds_scaled = map(tf, folds)
    return folds_scaled
    
def split_scale(df, target, split_on, split_values, drop=True):
    folds = custom_folds(df, split_on, split_values)
    scaler = fit_scaler(df, split_on, split_values, drop=drop)
    if drop == True:
        df = df.drop(split_on, axis=1)
    folds_scaled = scale_folds(df, target, folds, scaler)
    return folds_scaled

def fold_preds(fold, model, type, imbal=False):
    
    x = fold['x_train']
    y = fold['y_train']
        
    # use resampled data if data is imbalanced
    if imbal == True:
        sampler = RandomOverSampler(random_state=RANDOM_SEED)
        x, y = sampler.fit_resample(x, y)
    
    model.fit(x, y)
            
    if type == 'classification':
        preds =  model.predict_proba(fold['x_test'])[:, 1]
    else:
        preds =  model.predict(fold['x_test'])
    
    preds = [round(x, 4) for x in preds]
    fold['p_test'] = preds
    
    return fold


def convert_grid(id, space_grid):
    trial_params = space_grid[id]
    search_space = {}
    for p in trial_params['hyperparameters']:
        if trial_params['hyperparameters'][p]['func'] != 'choice':
            minv = trial_params['hyperparameters'][p]['min']
            maxv = trial_params['hyperparameters'][p]['max']
            
            if trial_params['hyperparameters'][p]['func'] == 'loguniform':
                o = hp.loguniform(p, np.log(minv), np.log(maxv))
                search_space[p] = o
        else:
            o = hp.choice(p, trial_params['hyperparameters'][p]['options'])
            search_space[p] = o

    return search_space

def hyper_search(grid_id, folds, n_trials, scoring, imbal=False):
    
    score_func = scorer_grid[scoring]['function']
    pred_type = scorer_grid[scoring]['type']
    
    target_vals = len(set(folds[0]['y_test']))
    
    if target_vals > 2:
        type = 'regression'
    else:
        type = 'classification'
    
    def objective_min(params):
        
        model.set_params(**params)
        
        predict_fold = lambda x: fold_preds(x, model, type, imbal=False)
        folds = map(predict_fold, folds)
        preds_list = [x['p_test'] for f in folds]
        preds = np.concatenate(preds_list).ravel().tolist()
        
        targets_list = [x['y_test'] for x in folds]
        # flatten list of lists to single list of targets
        targets = [i for sub in targets_list for i in sub]

        if type == 'classification':
            preds = np.where(preds > 0.50, 1, 0)

        cv_score = score_func(targets, preds)
        
        print "Trial: %r" % (len(trials.trials))
        
        loss = cv_score
        
        if pred_type == 'classification':
            loss = 1 - loss
        
        return {'loss':  loss,
                'score': cv_score,
                'status': STATUS_OK,
                'params': params,
                'grid_id': grid_id,
                'scorer': score_func.__name__}
    
    trials = Trials()
    
    model = model_grid[space_grid[grid_id]['model']]
    
    search_space = convert_grid(grid_id, space_grid)

    best = fmin(objective_min,
                search_space,
                algo=tpe.suggest,
                max_evals=n_trials,
                trials=trials)

    return trials


def dump_search(grid_id, trials):
    datdir_search = "../data/results/searches/"
    p_file = "".join([datdir_search, "search_", str(grid_id), ".p"])
    pickle.dump(trials, open(p_file, "wb"))

def write_results(grid_id, trials):
    
    # get result dict of trial with minimum loss
    imin = trials.losses().index(min(trials.losses()))
    top = trials.results[imin]
    
    # to include model identifier
    top['model'] = space_grid[grid_id]['model']
    
    # file to store scores and model metadata
    datdir_results = "../data/results/"
    s_file = "".join([datdir_results, "scores.json"])
    
    # open results logging json file if it exists
    try:
        with open(s_file) as f:
            scores = json.load(f)
    except:
        scores = {}
    
    # update scores dict with results of current run
    scores[str(grid_id)] = top

    # save results file as json
    with open(s_file, 'w') as f:
        json.dump(scores, f, indent=4)
    
    # return dict for other inspection, plotting, etc.
    return scores

def get_search(grid_id):
    datdir_search = "../data/results/searches/"
    p_file = "".join([datdir_search, "search_", str(grid_id), ".p"])
    with open(p_file, 'rb') as f:
        trials = pickle.load(f)
    return trials


def extract_trial(trial):
    keys_keep = ['tid']
    result = trial['result']
    trial_d = {your_key: trial[your_key] for your_key in keys_keep }
    trial_d.update(result['params'])
    del result['params']
    trial_d.update(result)
    return trial_d

def trials_data(trials, grid_id):
    trial_list = trials.trials
    trial_results = [extract_trial(x) for x in trial_list]
    df = pd.DataFrame(trial_results)
    df['model'] = space_grid[grid_id]['model']
    df = df.sort_values("loss", ascending=True)
    return df

def get_grid_result(grid_id):
    # file to store scores and model metadata
    datdir_results = "../data/results/"
    s_file = "".join([datdir_results, "scores.json"])

    # open results logging json file if it exists
    with open(s_file) as f:
        scores = json.load(f)
        
    return scores[str(grid_id)]
    
def model_set(grid_result):
    model_name = grid_result['model']
    model_params = grid_result['params']

    if model_name == 'logistic':
        params = {'C': 1.0, 'penalty': 'l2'}
        for p in model_params:
            params[p] = model_params[p]
        
        model = LogisticRegression(random_state=RANDOM_SEED,
                                   solver='liblinear',
                                   penalty=params['penalty'],
                                   C=params['C'])
    elif model_name == 'ridge':
        params = {'alpha': 1.0}
        for p in model_params:
            params[p] = model_params[p]
        
        model = Ridge(random_state=RANDOM_SEED,
                      solver='auto', alpha=params['alpha'])
    else:
        model = None
    return model
