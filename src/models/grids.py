import numpy as np
from hyperopt import hp
from sklearn.metrics import f1_score, accuracy_score
from sklearn.linear_model import LogisticRegression

scorer_grid = {}
scorer_grid['accuracy'] = accuracy_score
scorer_grid['f1'] = f1_score

model_grid = {}
model_grid['logistic'] = LogisticRegression(solver='liblinear',
                                            penalty='l2')


space_grid = {}

space_grid[0] = {"model": "logistic",
                 "hyperparameters": {"C": {"min": 0.00001,
                                           "max": 1,
                                           "func": "loguniform"
                                           }
                                     }
                 }
