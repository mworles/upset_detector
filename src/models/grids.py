import numpy as np
from hyperopt import hp
import sklearn.metrics as metrics
from sklearn.linear_model import LogisticRegression, Ridge, Lasso
from Constants import RANDOM_SEED

scorer_grid = {}
scorer_grid['accuracy'] = {'function': metrics.accuracy_score,
                           'type': 'classification'}
scorer_grid['f1'] = {'function': metrics.f1_score,
                     'type': 'classification'}
scorer_grid['precision'] = {'function': metrics.precision_score,
                            'type': 'classification'}
scorer_grid['recall'] = {'function': metrics.recall_score,
                            'type': 'classification'}
scorer_grid['MSE'] = {'function': metrics.mean_squared_error,
                      'type': 'regression'}
scorer_grid['MAE'] = {'function': metrics.mean_absolute_error,
                      'type': 'regression'}

model_grid = {}
model_grid['logistic'] = LogisticRegression(random_state=RANDOM_SEED,
                                            solver='liblinear')
model_grid['logistic_l1'] = LogisticRegression(random_state=RANDOM_SEED,
                                               solver='liblinear',
                                               penalty='l1')
model_grid['logistic_l2'] = LogisticRegression(random_state=RANDOM_SEED,
                                               solver='liblinear',
                                               penalty='l2')
model_grid['ridge'] = Ridge(random_state=RANDOM_SEED, solver='auto')
model_grid['lasso'] = Lasso(random_state=RANDOM_SEED)


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

space_grid[2] = {"model": "ridge",
                 "hyperparameters": {"alpha": {"min": 0.001,
                                               "max": 10,
                                               "func": "loguniform"
                                               },
                                     }
                 }

space_grid[3] = {"model": "lasso",
                 "hyperparameters": {"alpha": {"min": 0.001,
                                               "max": 20,
                                               "func": "loguniform"
                                               },
                                     }
                 }

space_grid[4] = {"model": "logistic_l1",
                 "hyperparameters": {"C": {"min": 0.1,
                                           "max": 10,
                                           "func": "loguniform"
                                           }
                                     }
                 }

space_grid[5] = {"model": "logistic_l2",
                 "hyperparameters": {"C": {"min": 0.0001,
                                           "max": 10,
                                           "func": "loguniform"
                                           }
                                     }
                 }
