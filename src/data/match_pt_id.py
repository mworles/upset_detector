import os
import pandas as pd
import numpy as np
import sys
sys.path.append("../")
from Cleaning import write_file, combine_files, clean_school_name, fuzzy_match
from Constants import COLUMNS_TO_RENAME

directory = '../../data/external/pt/'
df = combine_files(directory)

# list of all unique team names
teams = list(set(list(df['home']) + list(df['road'])))

df = pd.DataFrame({'team': teams})
df = df[df['team'].notnull()]

# count to compare below for success of matching
n_rows = df.shape[0]

df['team_clean'] = map(clean_school_name, df['team'].values)


# import team id number data
ns = pd.read_csv('../../data/raw/TeamSpellings.csv')
# rename some columns for code compatibility
ns = ns.rename(columns=COLUMNS_TO_RENAME)

nsd = ns.set_index('name_spelling')['team_id'].to_dict()

def get_id(team, id_dict, fuzzy=False):
    try:
        tid = id_dict[team]
        result = (team, tid)
    except:
        result = (team, None)
    if fuzzy==True and result[1] is None:
        fm = fuzzy_match(x, id_dict.keys())
        if fm != None:
            tid = id_dict[fm]
            result = (team, tid)
    return result

matches = [get_id(x, nsd) for x in df['team_clean']]
matches = [x for x in matches if x[1] is not None]
matched_ids = [x[1] for x in matches]
matched_teams = [x[0] for x in matches]

teams_remain = [x for x in df['team_clean'] if x not in matched_teams]

nsd_remain = {k: v for (k, v) in nsd.items() if v not in matched_ids}
matches_remain = [get_id(x, nsd, fuzzy=True) for x in teams_remain]
matches_remain = [x for x in matches_remain if x[1] is not None]

matches.extend(matches_remain)

md =  {k: v for (k, v) in matches}

df['team_id'] = df['team_clean'].apply(lambda x: get_id(x, md)[1])

print df.sort_values('team_id').tail(10)


"""
df['team_id'] = 


for t in df['team_clean'].values[0:50]:
    if t in nsd.keys():
        tid = nsd[t]
    else:
        tid = np.nan
    print tid


schools = clean_schools(directory)
df = match_school_id(schools)
df = df.rename(columns={'School': 'ss_team'})

data_out = '../../data/interim/'

# save school stats data file
write_file(df, data_out, 'id_school')
"""
