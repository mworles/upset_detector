import json
import pandas as pd
import sys
sys.path.append("../")
from Cleaning import write_file

def file_name(year):
    datdir = '../../data/external/odds/'
    year_file = "".join([datdir, "odds", "_", str(year), ".json"])
    return year_file

def year_games(year):
    year_file = file_name(year)
    year_games = []

    with open(year_file) as f:
        year_data = json.load(f)

    pages = year_data.keys()

    for p in pages:
        page_data = year_data[p]
        year_games.extend(page_data)

    return year_games

def change_date(x):
    dl = x.split('/')
    y = dl[-1]
    m = dl[0]
    d = dl[1]
    ds = "_".join([y, m, d])
    return ds

all_games = []

for year in range(2009, 2019):
    yg = year_games(year)
    all_games.extend(yg)

def encode_line(line):
    line_encoded = [x.encode('ascii', 'ignore') for x in line]
    return line_encoded

all = [encode_line(x) for x in all_games]

col_names = ['date', 'team1', 'team2', 'odds1', 'odds2']

df = pd.DataFrame(all, columns=col_names)

write_file(df, '../../data/external/odds/', 'odds', keep_index=False)
