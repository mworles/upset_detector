import pandas as pd
from models import utils
from data import clean
from constants import TEST_YEAR

def ids_from_index(df, full_date = False):
    """Returns the input dataframe with team id columns added. Team id numbers 
    are extracted from the dataframe index. Useful when team identifers have 
    been removed from data (i.e., for model training) but need to be 
    re-inserted for some reason, such as merging with other team data. 
    
    Arguments
    ----------
    df: pandas dataframe
        Requires an index of unique game identifers that contain team id for 
        both teams in the game.
    """
    # ensure index has name
    df.index = df.index.rename('game_id')
    # set index as column
    df = df.reset_index()
    
    # assume game date contains year only
    if full_date == False:
        df['t1_team_id'] = df['game_id'].apply(lambda x: int(x[5:9]))
        df['t2_team_id'] = df['game_id'].apply(lambda x: int(x[10:]))
    # if full date, need 
    else:
        df['t1_team_id'] = df['game_id'].apply(lambda x: int(x[11:15]))
        df['t2_team_id'] = df['game_id'].apply(lambda x: int(x[16:]))
    
    # return game identifer to index
    df = df.set_index('game_id')
    
    return df


def add_team_name(df, datdir='../data/'):
    """Returns the input dataframe with team names added. Team names are read 
    in from a file and merged with the input data using team identifers.
    
    Arguments
    ----------
    df: pandas dataframe
        Requires team identifer columns 't1_team_id' and 't2_team_id'. 
    datadir: string
        Relative path to data directory.
    """    
    # specificy path to team name data and read in dataframe
    path = "".join([datdir, 'scrub/teams.csv'])
    nm = pd.read_csv(path)
    
    nm = nm[['team_id', 'team_name']]
    
    # merge and create name column for team 1
    mrg = pd.merge(df, nm, left_on='t1_team_id', right_on='team_id',
                   how='inner')
    mrg = mrg.drop(columns=['team_id'])
    mrg = mrg.rename(columns={'team_name': 'team_1'})
    
    # merge and create name column for team 2
    mrg = pd.merge(mrg, nm, left_on='t2_team_id', right_on='team_id',
                   how='inner')
    mrg = mrg.drop(columns=['team_id'])
    mrg = mrg.rename(columns={'team_name': 'team_2'})
    
    
    return mrg

def switch_ids(df, toswitch):
    """Returns the input dataframe with team identifers switched in specified
    rows as indicated by input boolean array. Useful when the intent is to 
    organize data for presentation, such as when aligning all underdogs.
    
    Arguments
    ----------
    df: pandas dataframe
        Requires team identifer columns 't1_team_id' and 't2_team_id'. 
    toswitch: array
        Contains boolean values where True indicates rows to switch.
    """
    # copy of data for replacing values    
    dfr = df.copy()
    # switch both team identifers
    dfr.loc[toswitch, 't1_team_id'] = df.loc[toswitch, 't2_team_id']
    dfr.loc[toswitch, 't2_team_id'] = df.loc[toswitch, 't1_team_id']
    
    return dfr


# define data datdirectory, import features and targets
datdir = '../data/processed/'
df = pd.read_csv(datdir + 'features.csv', index_col=0)
targets = pd.read_csv(datdir + 'targets.csv', index_col=0)

# input variable values
split_values = [TEST_YEAR]
split_on = 'season'
target = targets['upset']
grid_id = 1

# remove examples missing the target
has_target = targets[targets['upset'].notnull()].index.values
df = df[df.index.isin(has_target)]

# remove examples beyond test year
df = df[df['season'] <= TEST_YEAR]

# array of rows to switch for upset seed order
toswitch = clean.upset_switch(df)

# re-arrange data for predicting upsets
df = clean.upset_features(df)

# split dataset into cross-validation folds and scale data
folds_scaled = utils.split_scale(df, target, split_on, split_values)

# set up model from grid search result
grid_result = utils.get_grid_result(grid_id)
model_name = grid_result['model']
model = utils.model_set(grid_result)

# get model predictions
probs_list = utils.fold_preds(folds_scaled[0], model, type='probs', imbal=True)

# list of indices to isolate test set examples
i_test = utils.fold_split(df, 'season', TEST_YEAR)['i_test']

# isolate examples to team identifiers
test = df[df.index.isin(i_test)]
test = clean.ids_from_index(test)
test = test[['t1_team_id', 't2_team_id']]

test = clean.switch_ids(test, toswitch)

# add team names to data
test = clean.add_team_name(test, datdir='../data/')

test['t1_prob'] = probs_list
test['uprob'] = (test['t1_prob'] * 100).astype(int)

test['uprob'] = test['uprob'].astype(str).apply(lambda x: x + ' %')

test = test.drop(['t1_team_id', 't2_team_id', 't1_prob'], axis=1)
cols_rename = {'team_1': 'Underdog',
               'team_2': 'Favorite',
               'uprob': 'Upset probability'}
test = test.rename(columns=cols_rename)

test.to_html('../post/' + 'upsets_2018.html', index=False)
