import pandas as pd
import numpy as np
import json
import pickle
from sklearn.preprocessing import StandardScaler
from hyperopt import fmin, tpe, Trials, space_eval, hp, STATUS_OK
from models.grids import model_grid, space_grid, scorer_grid

def fold_split(df, split_on, split_value):
    """Split data into one training and validation set. 
    
    Training set is all examples where split_on is less than split_value. 
    Validation set is all examples where split_on is equal to split_value.
    
    Args:
        df: a dataframe that includes split_on as a column
        split_on: column to use to split
        split_value: value used to split
    """
    # identify index values for training set and test set
    train = df[df[split_on] < split_value].index.values
    test = df[df[split_on] == split_value].index.values
    
    # return dict 
    dict_split = {'fold': split_value, 'i_train': train, 'i_test': test}
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
        scale_data = df.drop(split_on, axis=1)
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
    return fd

def scale_folds(df, target, folds, scaler):
    tf = lambda x: scale_fold(df, target, x, scaler)
    folds_scaled = map(tf, folds)
    return folds_scaled

def fold_score(fold, model, score_func):
    
    model.fit(fold['x_train'], fold['y_train'])
    
    pv =  model.predict_proba(fold['x_test'])[:, 1]
    lv = np.where(pv > 0.50, 1, 0)

    score_val = score_func(fold['y_test'], lv)

    return score_val

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

def hyper_search(grid_id, folds, n_trials, score_type):
    
    score_func = scorer_grid[score_type]
    
    def objective_min(params):
        
        model.set_params(**params)
        scores = map(lambda x: fold_score(x, model, score_func), folds)
        mean_score = np.mean(scores)
        print "Trial: %r" % (len(trials.trials))
        
        return {'loss': 1 - mean_score,
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
    dir_search = "../data/results/searches/"
    p_file = "".join([dir_search, "search_", str(grid_id), ".p"])
    pickle.dump(trials, open(p_file, "wb"))


def write_results(grid_id, trials):
    
    # get result dict of trial with minimum loss
    imin = trials.losses().index(min(trials.losses()))
    top = trials.results[imin]
    
    # to include model identifier
    top['model'] = space_grid[0]['model']
    
    # file to store scores and metadata
    dir_results = "../data/results/"
    s_file = "".join([dir_results, "scores.json"])
    
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
    dir_search = "../data/results/searches/"
    p_file = "".join([dir_search, "search_", str(grid_id), ".p"])
    with open(p_file, 'rb') as f:
        trials = pickle.load(f)
    return trials

def trials_data(trials, search_space):
    param_values = [x['misc']['vals'] for x in trials.trials]
    param_values = [{key: value for key in x for value in x[key]}
                    for x in param_values]
    param_values = [space_eval(search_space, x) for x in param_values]
    losses = [x['result']['loss'] for x in trials.trials]
    
    trials_data = param_values
    [x.update({'loss': y}) for x, y in zip(trials_data, losses)]
    
    df = pd.DataFrame(param_values)
    return df

def split_scale(df, target, split_on, split_values, drop=True):
    folds = custom_folds(df, split_on, split_values)
    scaler = fit_scaler(df, split_on, split_values, drop=drop)
    if drop == True:
        df = df.drop(split_on, axis=1)
    folds_scaled = scale_folds(df, target, folds, scaler)
    return folds_scaled
