import sys
sys.path.append("../")
import pandas as pd
import numpy as np
from Cleaning import set_gameid_index, convert_team_id, add_seeds, write_file
from Constants import COLUMNS_TO_RENAME

def get_score(row, team_id, score_dict):
    row_gameid = row.name
    row_team = row[team_id]
    team_score = score_dict[row_gameid][row_team]
    return team_score

def team_id_scores(df):
    # create dict with key as game identifier, values as team scores
    score_dict = {}
    for i, r in df.iterrows():
        score_dict[i] = {r['wteam']: r['wscore'], r['lteam']: r['lscore']}

    df = convert_team_id(df, tc)

    df['t1_score'] = df.apply(lambda x: get_score(x, 't1_team_id', score_dict),
                              axis=1)
    df['t2_score'] = df.apply(lambda x: get_score(x, 't2_team_id', score_dict),
                              axis=1)
    return df
    
def get_t1_win(df, id_cols):
    df = get_scores(df, id_cols)
    df['t1_win'] = np.where(df['t1_score'] > df['t2_score'], 1, 0)
    df = df.drop(columns=['t1_score', 't2_score'])
    return df
    

def get_upsets(idx):
    df = pd.read_csv('../../data/raw/NCAATourneyCompactResults.csv')
    df.columns = map(lambda x: x.lower(), df.columns)

    # assigns t1_seed to winning teams, t2_seed to losing teams
    df = add_seeds('../../data/raw/', df, ['wteamid', 'lteamid'])

    df = set_gameid_index(df, ['wteamid', 'lteamid'])
    df = df[df.index.isin(idx)]
    
    def label_row(row):
        # absolute seed difference 3 or less, no upset assigned
        if abs(row.t1_seed - row.t2_seed) <= 3:
            return np.nan
        # seed difference 3 or greater, assign 1 for upset
        elif (row.t1_seed - row.t2_seed) > 3:
            return 1
        # assign 0 for no upset
        else:
            return 0
    
    upset = df.apply(label_row, axis=1)
    
    return pd.DataFrame({'upset': upset}, index=df.index)

dir = '../../data/'
df = pd.read_csv(dir + 'raw/NCAATourneyCompactResults.csv')
df = df.rename(columns=COLUMNS_TO_RENAME)
df.columns = df.columns.str.lower()

tc = ['wteam', 'lteam']

# set index as unique game identifier
df = set_gameid_index(df, tc)

df = team_id_scores(df)

df['t1_win'] = np.where(df['t1_score'] > df['t2_score'], 1, 0)

df['t1_marg'] = df['t1_score'] - df['t2_score']

upset = get_upsets(df.index.values)
df = pd.merge(df, upset, how='left', left_index=True, right_index=True)

# keep target columns
df = df.loc[:, ['t1_win', 't1_marg', 'upset']]

# save file
write_file(df, dir + '/processed/', 'targets', keep_index=True)
