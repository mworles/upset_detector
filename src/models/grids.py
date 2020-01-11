import numpy as np
from hyperopt import hp
from sklearn.metrics import f1_score, accuracy_score
from sklearn.linear_model import LogisticRegression
from Constants import RANDOM_SEED

scorer_grid = {}
scorer_grid['accuracy'] = accuracy_score
scorer_grid['f1'] = f1_score

scorer_pred = {}
scorer_pred['accuracy'] = 'labels'
scorer_pred['f1'] = 'labels'

model_grid = {}
model_grid['logistic'] = LogisticRegression(random_state=RANDOM_SEED,
                                            solver='liblinear')


space_grid = {}

space_grid[0] = {"model": "logistic",
                 "hyperparameters": {"C": {"min": 0.00001,
                                           "max": 1,
                                           "func": "loguniform"
                                           },
                                     "penalty": {"options": ['l1', 'l2'],
                                                "func": "choice"
                                                }
                                     }
                 }

space_grid[1] = {"model": "logistic",
                 "hyperparameters": {"C": {"min": 0.01,
                                           "max": 10,
                                           "func": "loguniform"
                                           },
                                     "penalty": {"options": ['l1'],
                                                "func": "choice"
                                                }
                                     }
                 }
