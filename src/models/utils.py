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
from imblearn.over_sampling import RandomOverSampler
from constants import RANDOM_SEED

def score_to_class(x):
    if x[0] == float("nan"):
        return float("nan")
    if x[1] >= x[0]:
        return 1
    else:
        return 0
        
def target_from_prob(x, threshold=0.50):
    if x >= threshold:
        return 1
    else:
        return 0

def beta_calibrate(x, a, c):
    prob_calib = 1/(1 + 1/(np.exp(c)*(x**a/(1-x)**a) ))
    return prob_calib

def apply_beta_calibration(target_train, probs_train, probs_test):

    lr = LogisticRegression(C=10000)
    sprm = np.log(probs_train/(1 - probs_train))
    lr.fit(sprm.reshape(-1, 1), target_train)
    c = lr.intercept_[0]
    a = lr.coef_[0][0]

    probs_calib = np.array(map(lambda x: beta_calibrate(x, a, c), probs_test))
    return probs_calib

        
def roi(targets, preds, bet=100):
    def return_on_bet(x, bet=100):
        if x[0]==x[1]:
            return bet * 1.90
        else:
            return 0
    bet = 100
    returns = map(lambda x: return_on_bet(x, bet), zip(targets, preds))
    return_total = sum(returns)
    bet_total = bet * len(preds)
    net_total = return_total - bet_total
    roi = net_total / bet_total
    return roi
            

def score_spreads(folds):
    preds_all = [x for fold in folds for x in fold['p_test']]
    targets_all = [x for fold in folds for x in fold['y_test']]
    spreads_all = [-x if x !=0 else x for fold in folds for x in fold['spreads_test']]
    
    spreads_targets = zip(spreads_all, targets_all)
    spreads_preds = zip(spreads_all, preds_all)

    covers_true = map(score_to_class, spreads_targets)
    covers_pred = map(score_to_class, spreads_preds)
    
    true_pred =  zip(covers_true, covers_pred)
    true_pred_use = [x for x in true_pred if x[0] != float('nan')]
    
    y_true = [x[0] for x in true_pred_use]
    y_pred = [x[1] for x in true_pred_use]

    return y_true, y_pred

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
    
def split_scale(df, split_on, split_values, drop=True):
    # create list of dicts with index values
    target = df['target']
    df = df.drop(['target'], axis=1)
    folds = custom_folds(df, split_on, split_values)
    # fit scaler to uniform data for all folds
    scaler = fit_scaler(df, split_on, split_values, drop=drop)
    if drop == True:
        df = df.drop(split_on, axis=1)

    # list of k dicts with scaled train and test data 
    folds_scaled = scale_folds(df, target, folds, scaler)
    return folds_scaled

def fold_preds(fold, model, type, calibrate=True, imbal=False):
    
    x = fold['x_train']
    y = fold['y_train']
        
    # use resampled data if data is imbalanced
    if imbal == True:
        sampler = RandomOverSampler(random_state=RANDOM_SEED)
        x, y = sampler.fit_resample(x, y)
    
    model.fit(x, y)
        
    if type == 'classification':
        preds =  model.predict_proba(fold['x_test'])[:, 1]
        if calibrate==True:
            preds_train = model.predict_proba(x)[:, 1]
            preds = apply_beta_calibration(y, preds_train, preds)
    else:
        preds =  model.predict(fold['x_test'])
    
    preds = [round(x, 4) for x in preds]
        
    return preds


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

def hyper_search(grid_id, folds, n_trials, score_type, convert=None,
                 imbal=False):
    
    score_func = scorer_grid[score_type]['function']

    def objective_min(params):
        
        target_vals = len(set(folds[0]['y_test']))
    
        if target_vals > 2:
            type = 'regression'
        else:
            type = 'classification'

        model.set_params(**params)
        
        for f in folds:
            f['p_test'] = fold_preds(f, model, type=type, imbal=imbal)
        
        targets = [x for fold in folds for x in fold['y_test']]
        preds = [x for fold in folds for x in fold['p_test']]

        if convert == 'spreads':
            targets, preds = score_spreads(folds)
            type = 'classification'
        else:
            pass
        
        if type == 'classification' and convert == None:
            preds = map(lambda x: target_from_prob(x), preds)
        
        cv_score = score_func(targets, preds)

        if type == 'classification':
            loss = 1 - cv_score
        
        print "Trial: %r" % (len(trials.trials))

        return {'loss': loss,
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

def write_results(exp_id, grid_id, trials):
    
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
    scores[str(exp_id)] = top

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
