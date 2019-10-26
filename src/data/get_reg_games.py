import sys
sys.path.append("../")
import os
import pandas as pd
import numpy as np
from Cleaning import write_file


def parse_winners(x):
    wlist = x[wcoli].tolist() + x[lcoli].tolist()
    wlist[0:0] = [x[0], x[6], x[1], x[7]]
    return wlist

def parse_losers(x):
    llist = x[lcoli].tolist() + x[wcoli].tolist()
    llist[0:0] = [x[0], LLoc_map[x[6]], x[1], x[7]]
    return llist

print 'running %s' % (os.path.basename(__file__))

# read in data files
data_in = '../../data/raw/'

reg_det = pd.read_csv(data_in + 'RegularSeasonDetailedResults.csv')
reg_com = pd.read_csv(data_in + 'RegularSeasonCompactResults.csv')
reg_com = reg_com[reg_com.Season < reg_det['Season'].min()]
rgames = pd.concat([reg_det, reg_com], sort=False)

wcols = [x for x in rgames.columns if x[0] == 'W' and x != 'WLoc']
lcols = [x for x in rgames.columns if x[0] == 'L']
LLoc_map = {'H': 'A', 'A': 'H', 'N': 'N'}

new_cols = ['season', 't1_loc', 'daynum', 'numot']
new_cols.extend([x.replace('W', 't1_') for x in wcols])
new_cols.extend([x.replace('L', 't2_') for x in lcols])
new_cols = [x.lower() for x in new_cols]

# %%
wcoli = [list(rgames.columns).index(x) for x in wcols]
lcoli = [list(rgames.columns).index(x) for x in lcols]

# %%
rgames_array = rgames.values
winners = map(parse_winners, rgames_array)
losers = map(parse_losers, rgames_array)
games = winners + losers

df = pd.DataFrame(games, columns=new_cols)

df = df.sort_values(['season', 'daynum', 't1_teamid'])

data_out = '../../data/raw/'
file_name = 'reg_games'
write_file(df, data_out, file_name)
