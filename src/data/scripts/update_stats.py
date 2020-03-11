from src.data import features
from src.data import Transfer

df = Transfer.return_data('game_box')
df = features.team.split_teams(df)
print df.sort_values(['team', 'date']).head(20)
df = features.team.clean_box(df)
print df.sort_values(['team', 'date']).head(20)
