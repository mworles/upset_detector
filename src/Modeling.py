import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import f1_score, accuracy_score

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

def score_fold(fold, model, score_func):
    
    model.fit(fold['x_train'], fold['y_train'])
    
    pv =  model.predict_proba(fold['x_test'])[:, 1]
    lv = np.where(pv > 0.50, 1, 0)

    score_val = score_func(fold['y_test'], lv)

    return {fold['fold']: score_val}

def fit_scaler(df):
    sc = StandardScaler()
    sc.fit(scale_data.astype(float))
    return sc

# define data directory, import features and targets
dir = '../data/processed/'
df = pd.read_csv(dir + 'features.csv', index_col=0)
targets = pd.read_csv(dir + 'targets.csv', index_col=0)

# input variable values
fold_vals = [2014, 2015, 2016, 2017, 2018]
fold_col = 'season'
target_name = 't1_win'
model = LogisticRegression(solver='liblinear')
score_func = accuracy_score


# use a data column to create custom cross validation folds
folds = custom_folds(df, fold_col, fold_vals)

# fit data scaler to training examples
fold_min = min(fold_vals)
scale_data = df[df[fold_col] < fold_min]
scale_data = df.drop(fold_col, axis=1)
scaler = fit_scaler(scale_data)

# define target
target = targets[target_name]

# scale data and score each fold
for f in folds:
    ds_f = fold_data(scale_data, target, f, scaler)
    score_f = score_fold(ds_f, model, score_func)
    print score_f
