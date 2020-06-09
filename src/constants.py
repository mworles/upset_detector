import os

# year to use as test set
TEST_YEAR = 2020

# minimum year to use for data inclusion, due to missing data
MIN_YEAR = 2003

# list of years to use as validation sets in chronological cross-validation
SPLIT_YEARS = [2015, 2016, 2017, 2018, 2019]

# seed number to use for random data processses
RANDOM_SEED = 40195

# create absolute path to this file
PATH_HERE = os.path.abspath(os.path.dirname(__file__))

CONFIG = '../.config'
CONFIG_FILE= os.path.join(PATH_HERE, CONFIG)

SCHEMA = 'data/table_schema.json'
SCHEMA_FILE = os.path.join(PATH_HERE, SCHEMA)

SOURCE_ID_YEARS = {'team_ss': 1993,
                   'team_kp': 2002,
                   'team_pt': 2003,
                   'team_oddsport': 2009,
                   'team_tcp': 2019,
                   'team_vi_odds': 2019,
                   'team_vi_spreads': 2019,
                   'team_sbro': 2009}

S3_BUCKET = 'worley-upset-detector-public'
