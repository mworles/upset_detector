import Constants
from data import Generate

datdir = Constants.DATA

# read in data file with game results
file = datdir + '/scrub/ncaa_results.csv'
df = pd.read_csv(file)

# created outcome-neutral team identifier
df = convert_team_id(df, ['wteam', 'lteam'], drop=False)
# create unique game identifier and set as index
df = set_gameid_index(df)
# add column indicating score for each team
scores = team_scores(df)

print scores.shape
