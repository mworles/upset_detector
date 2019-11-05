import pandas as pd
import numpy as np
import json
import pickle
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import f1_score, accuracy_score
from hyperopt import fmin, tpe, Trials, space_eval, hp, STATUS_OK
from models.search.models import models
from models.search.search_params import search_input

def fold_indices(df, column, value):
    train = df[df[column] < value].index.values
    test = df[df[column] == value].index.values
    fd = {'fold': value,
          'i_train': train,
          'i_test': test}
    return fd

def custom_folds(df, column, values):
    x = map(lambda x: fold_indices(df, column, x), values)
    return x

def fold_data(df, target, fold, scaler):
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

def fold_score(fold, model, score_func):
    
    model.fit(fold['x_train'], fold['y_train'])
    
    pv =  model.predict_proba(fold['x_test'])[:, 1]
    lv = np.where(pv > 0.50, 1, 0)

    score_val = score_func(fold['y_test'], lv)

    return score_val

def fit_scaler(df):
    sc = StandardScaler()
    sc.fit(scale_data.astype(float))
    return sc
    
    
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

# define data directory, import features and targets
dir = '../data/processed/'
df = pd.read_csv(dir + 'features.csv', index_col=0)
targets = pd.read_csv(dir + 'targets.csv', index_col=0)

# input variable values
fold_vals = [2014, 2015, 2016, 2017, 2018]
fold_col = 'season'
target_name = 't1_win'
#model = LogisticRegression(solver='liblinear')
score_func = accuracy_score


# use a data column to create custom cross-validation folds
folds = custom_folds(df, fold_col, fold_vals)

# fit data scaler to training examples
fold_min = min(fold_vals)
scale_data = df[df[fold_col] < fold_min]
scale_data = df.drop(fold_col, axis=1)
scaler = fit_scaler(scale_data)

# define target
target = targets[target_name]

# scale data and score each fold
af = map(lambda x: fold_data(scale_data, target, x, scaler), folds)
#scores = map(lambda x: fold_score(x, model, score_func), af)

with open("models/search/params.json") as f:
    search_set = json.load(f)


def get_space(trial_n, search_input):
    trial_params = search_input[trial_n]
    search_space = {}
    for p in trial_params['hyperparameters']:
        minv = trial_params['hyperparameters'][p]['min']
        maxv = trial_params['hyperparameters'][p]['max']
        
        if trial_params['hyperparameters'][p]['func'] == 'loguniform':
            o = hp.loguniform(p, np.log(minv), np.log(maxv))
            search_space[p] = o

    return search_space


def hyper_search(folds, n_trials, score_func):
    
    def objective_min(params):
        
        model.set_params(**params)
        scores = map(lambda x: fold_score(x, model, score_func), folds)
        mean_score = np.mean(scores)
        print "Trial: %r" % (len(trials.trials))
        
        return {'loss': 1 - mean_score,
                'status': STATUS_OK,
                'params': params
                }
        #return (1 - mean_score)
    
    trials = Trials()
    
    with open("models/search/params.json") as f:
        params = json.load(f)
    
    
    model = models[search_input[0]['model']]
    
    search_space = get_space(0, search_input)

    best = fmin(objective_min,
                search_space,
                algo=tpe.suggest,
                max_evals=n_trials,
                trials=trials)
    
    pickle.dump(trials, open("../data/results/search_0.p", "wb"))

    return best


best = hyper_search(af, 50, score_func)

with open("../data/results/search_0.p", 'rb') as f:
    trials = pickle.load(f)



#print(trials_data(trials, search_space))
print trials.trials[0]
