from hyperopt import hp

search_input = {}

search_input[0] = {"model": "logistic",
                   "hyperparameters": {"C": {"min": 0.0001,
                                             "max": 1,
                                             "func": "loguniform"
                                             }
                                       }
                   }
