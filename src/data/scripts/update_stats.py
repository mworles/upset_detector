from src.data import features
from src.data import Transfer
from src.data import Generate
from src.data import Match

df = Transfer.return_data('game_box')
st = features.team.split_teams(df)
cb = features.team.clean_box(st)
sbt = Generate.games_by_team(cb)
sbt = sbt.rename(columns={'team_id': 'team_name'})
sbt = Match.id_from_name(sbt, 'team_tcp', 'team_name')


#df = prep_season(season, subset='ncaa'
