import sys
sys.path.append("../")
import pandas as pd
import numpy as np
from Cleaning import set_gameid_index, convert_team_id, add_seeds, write_file
from Cleaning import get_score, team_id_scores, get_t1_win, get_upsets
from Constants import COLUMNS_TO_RENAME

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
