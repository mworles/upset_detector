from data import Transfer, Updater, Clean
import queries
import pandas as pd
import numpy as np
import pickle

"""
df = Transfer.return_data('matchups')
feat_i = df.columns.tolist().index('t1_team_off_adj')
feat = df.iloc[:, feat_i:]
target = df['t1_marg'].values
feat['season'] = df['season']
"""
"""
from models import utils
grid_id = 2
# restore trials object from grid id
trials =  utils.get_search(grid_id)
df = utils.trials_data(trials, grid_id)
df = df.sort_index()
"""
"""
def run():
    import Constants
    from models import utils
    mat = Transfer.return_data('matchups')
    mat = mat.set_index('game_id')
    feat_i = mat.columns.tolist().index('t1_team_off_adj')
    df = mat.iloc[:, feat_i:]
    target = mat['t1_marg']
    # add season back in for cross-validation split
    df['season'] = mat['season']
    
    # input variable values
    split_values = Constants.SPLIT_YEARS
    split_on = 'season'
    score_type = 'MAE'
    grid_id = 2

    # split dataset into cross-validation folds and scale data
    folds = utils.split_scale(df, target, split_on, split_values)

    # set up model from grid search result
    grid_result = utils.get_grid_result(grid_id)
    model_name = grid_result['model']
    model = utils.model_set(grid_result)

    predict_fold = lambda x: utils.fold_preds(x, model, type, imbal=False)
    folds = map(predict_fold, folds)
    preds_list = [f['p_test'] for f in folds]
    targets_list = [f['y_test'] for f in folds]

    mod = "WHERE date > '2014/10/01'"
    spreads = Transfer.return_data('spreads_clean', mod)
    # drop missing
    spreads = spreads.dropna(subset=['t1_spread'])
    spreads = spreads.sort_values('game_id')
    get_season = lambda x: Clean.season_from_date(x)
    spreads['season'] = spreads['date'].apply(get_season)
    
    for f in folds:
        # limit spreads to fold year
        fs = spreads[spreads['season'] == f['fold']]
        # list of game ids in both spreads and test fold
        gid = list(set(f['i_test']) & set(fs['game_id'].values))
        gid.sort()
        
        # add ids to the fold
        f['i_spread'] = gid
        
        # limit spread data to games with predictions
        fs = fs[fs['game_id'].isin(gid)]
        
        # use zipped list to create lookup dict of spreads
        fgs = zip(fs['game_id'].values, fs['t1_spread'].values)
        fsd = {k:v for k, v in fgs}
        
        # lookup dict of the actual score
        fsy = {k:v for k, v in zip(f['i_test'], f['y_test'])}
        
        # lookup dict of the predicted score
        fsp = {k:v for k, v in zip(f['i_test'], f['p_test'])}
        
        f['spread'] = [[g, fsd[g], fsy[g], fsp[g]] for g in gid]

    spread_data = [['game_id', 'spread', 't1_marg', 'prediction']]
    
    for fold in folds:
        spread_data.extend(fold['spread'])

    with open('spread_data.pkl', 'wb') as f:
        pickle.dump(spread_data, f)

#run()

with open('spread_data.pkl', 'r') as f:
    spread_data = pickle.load(f)

df = pd.DataFrame(spread_data[1:], columns=spread_data[0])

df['spread'] = - df['spread']
df['prediction'] = df['prediction'].round(0)
for col in ['spread', 't1_marg', 'prediction']:
    df[col] = df[col].astype(float)


df['spread_bet'] = None
df['spread_bet'] = np.where(df['spread'] == df['prediction'], 0, None)
df = df[df['spread_bet'] !=0]

df['spread_bet'] = np.where(df['prediction'] > df['spread'] + 3, 1, df['spread_bet'])
df['spread_bet'] = np.where(df['prediction'] < df['spread'] - 3, 0, df['spread_bet'])

df['result'] = np.where(df['t1_marg'] == df['spread'], 1, None)
df['result'] = np.where(df['t1_marg'] > df['spread'], 1, df['result'])
df['result'] = np.where(df['t1_marg'] < df['spread'], 0, df['result'])

df = df.dropna(subset=['spread_bet', 'result'])
y_true = df['result'].astype(int)
y_pred = df['spread_bet'].astype(int)

from sklearn.metrics import accuracy_score
print accuracy_score(y_true, y_pred)
"""
"""
mat = Transfer.return_data('matchups')
mat = mat.set_index('game_id')
mat.to_pickle('mat.pkl')

s = Transfer.return_data('spreads_clean')
s = s.dropna(subset=['t1_spread'])
s = s[['game_id', 't1_spread']]
s.to_pickle('s.pkl')


mrg = pd.merge(mat, s, how='left', left_on='game_id', right_on='game_id')
mrg.to_pickle('mrg.pkl')
"""
from data import Transfer
import pandas as pd
"""
df = pd.read_pickle('mrg.pkl')
ns = df[df['t1_spread'].isnull()]
ns1 = ns[['game_id', 'date', 't1_team_id', 'season']]
ns1 = ns1.rename(columns={'t1_team_id': 'team_id'})

ns2 = ns[['game_id', 'date', 't2_team_id', 'season']]
ns2 = ns2.rename(columns={'t2_team_id': 'team_id'})

df = pd.concat([ns1, ns2], sort=False)
df['team_id'] = df['team_id'].astype(int)
df.to_pickle('df1.pkl')

df = pd.read_pickle('df1.pkl')
tk = Transfer.return_data('team_key')
tk = tk[['team_id', 'team_name']]
tk = tk.drop_duplicates()
tk['team_id'] = tk['team_id'].astype(int)

mrg = pd.merge(df, tk, left_on='team_id', right_on='team_id')
mrg.to_pickle('df2.pkl')
"""
mrg = pd.read_pickle('df2.pkl')
gb = mrg.groupby(['season', 'team_name'])['date'].count()
gb = gb.reset_index()
gb = gb.sort_values(['date'], ascending=False)
for season in range(2003, 2020):
    print gb[gb['season'] == season].head(10)
