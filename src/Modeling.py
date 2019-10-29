import pandas as pd
import numpy as np
from sklearn.model_selection import KFold

def fold_indices(df, column, value):
    train = df[df[column] < value].index.values
    test = df[df[column] == value].index.values
    return {'fold': value, 'train': train, 'test': test}

def custom_folds(df, column, values):
    x = map(lambda x: fold_indices(df, column, x), values)
    return x
    

def add_target(index_values):
    t = 
    if target == 'upset':
        
        df = rearrange_to_upset(df)
        df = label_upsets(df)
        df = df[df['upset'].notnull()]
    return df

dir = '../data/processed/'
df = pd.read_csv(dir + 'features.csv', index_col=0)

idx = df.index.values
targets = add_target(idx):

v = [2015, 2016, 2017]
c = 'season'

folds = custom_folds(df, c, v)
