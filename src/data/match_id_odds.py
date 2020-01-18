import os
import pandas as pd
import numpy as np
import sys
sys.path.append("../")
from Cleaning import write_file, combine_files, clean_school_name, fuzzy_match
from Constants import COLUMNS_TO_RENAME


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


datdir = "../../data/external/odds/"
df = pd.read_csv(datdir + 'odds.csv')

# list of all unique team names
teams = list(set(list(df['team1']) + list(df['team2'])))

team_clean = map(clean_school_name, teams)

# import team id number data
ns = pd.read_csv('../../data/raw/TeamSpellings.csv')
# rename some columns for code compatibility
ns = ns.rename(columns=COLUMNS_TO_RENAME)

nsd = ns.set_index('name_spelling')['team_id'].to_dict()

matches = [get_id(x, nsd) for x in team_clean]
matches = [x for x in matches if x[1] is not None]

matched_ids = [x[1] for x in matches]
matched_teams = [x[0] for x in matches]

teams_remain = [x for x in team_clean if x not in matched_teams]

matches_remain = [get_id(x, nsd, fuzzy=True) for x in teams_remain]
matches_remain = [x for x in matches_remain if x[1] is not None]

matches.extend(matches_remain)

md =  {k: v for (k, v) in matches}

team_id = map(lambda x: get_id(x, md)[1], team_clean)

df = pd.DataFrame({'team': teams, 'team_id': team_id})

data_out = '../../data/interim/'

# save school stats data file
write_file(df, data_out, 'odds_id')
