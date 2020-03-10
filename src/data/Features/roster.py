from src.data import Transfer
import pandas as pd
import numpy as np

def height_inches(x):
    '''Returns inches from string height.'''
    try:
        f = int(x.split('-')[0])
        i = int(x.split('-')[1])
        return (f * 12) + i
    except:
        return None

def get_starters(group):
    grp = group.sort_values('min_pg', ascending=False)
    grp['starter'] = 'no'
    grp.iloc[0:5, 0] = 'yes'
    return grp
    
"""
mod = ";"
df = Transfer.return_data('team_roster', modifier=mod)
df.to_pickle('df.pkl')

ppg = Transfer.return_data('player_pergame')
ppg.to_pickle('ppg.pkl')


ros = pd.read_pickle('df.pkl')
ppg = pd.read_pickle('ppg.pkl')


merge_on = ['season', 'team', 'name']
df = pd.merge(ros, ppg, how='inner', left_on=merge_on, right_on=merge_on)

# numeric height, in inches
df['height_num'] = map(height_inches, df['height'].values)

# position group
posdict = {'C': 'front', 'F': 'front', 'G': 'back'}
df["pos_grp"] = df['position'].map(posdict)

# reformat class 
df['class'].fillna('', inplace=True)
df['class'] = map(str.upper, df['class'])

# class group, upper or lower
classdict = {'SR': 'upper', 'JR': 'upper', 'SO': 'lower', 'FR': 'lower'}
df['class_grp'] = df['class'].map(classdict)

# numeric experience from class
expdict = {'SR': 3, 'JR': 2, 'SO': 1, 'FR': 0}
df["yrs_exp"] = df['class'].map(expdict)

df.to_pickle('df2.pkl')

df = pd.read_pickle('df2.pkl')

gbcols = ['season', 'team']


df['min_pg'] = df['min_pg'].astype(float)
df['g_start'] = df['g_start'].astype(float)
df['ph'] = 1
df = df.sort_values(['season', 'team', 'min_pg'],
                    ascending=[True, True, False])
df['min_rnk'] = df.groupby(['season', 'team'])['ph'].transform('cumsum')

df = df.sort_values(['season', 'team', 'g_start', 'min_rnk'],
                    ascending=[True, True, False, True])
df['gs_rnk'] = df.groupby(['season', 'team'])['ph'].transform('cumsum')
df['starter'] = np.where(df['gs_rnk'] < 6, 1, 0)

df.to_pickle('df3.pkl')
"""
df = pd.read_pickle('df3.pkl')

df['min_pct'] = df['min_pg'] / 200

import math
def weighted_height(x):
    if x.name[0] < 2002:
        h = x['height_num'].values
        h_mean = np.nanmean(h)
    else:
        min = x[x['min_pct'] > .10]
        h = x['height_num'].values
        w = min['min_pct'].values
        if len(w) == 0:
            h_mean = np.nanmean(h)
        else:
            h_w = zip(h, w)
            h_w = [v for v in h_w if not math.isnan(v[0])]
            h = [v[0] for v in h_w]
            w = [v[1] for v in h_w]
            h_mean = np.average(h, weights=w)
    return round(h_mean, 3)

df19 = df[df['season'] == 2019]
df19 = df19[df19['team'] == 'central-florida']
print df19[['name', 'height', 'height_num', 'min_pct']].sort_values(['height_num', 'min_pct'], ascending=[False, False])

"""
gb = df.groupby(['season', 'team'])

avg_height = gb.apply(lambda x: weighted_height(x))
dfah = avg_height.reset_index()

bigs = df[df['position'] != 'G']
gbb = bigs.groupby(['season', 'team'])

eff_height = gb.apply(lambda x: weighted_height(x))
dfeh = eff_height.reset_index()

df19 = dfeh[dfeh['season'] == 2019]
print df19.sort_values(0, ascending=False).head(20)
"""
