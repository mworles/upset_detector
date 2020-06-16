import pandas as pd
from src.data import transfer
from src.data import clean
from src.data import match
from src.data import players
from src.features import roster
from src.features import team
from src.features import ratings
import table_map

dba = transfer.DBAssist()


for table in table_map.KEY:
    file_name = '{}.csv'.format(table)
    df = clean.s3_data(file_name)
    df = clean.convert_raw_file(file_name)
    table_name = table_map.KEY[table]['new_name']

    try:
        dba.create_from_schema(table_name)
    except:
        dba.create_from_data(table_name, df)

    dba.insert_rows(table_name, df)

dba.create_from_schema('tourney_success')
df = team.tourney_performance()
dba.insert_rows('tourney_success', df)
ratings = ratings.compile()
dba.create_from_data('ratings', ratings)
dba.insert_rows('ratings', ratings)


team_key = match.run()
player_stats = players.clean_roster(min_season=2018)
team_stats = roster.run(player_stats)
games = team.games_regular()

# remove columns not needed for game stats
games = games.drop(columns=['wloc', 'numot'])
team_games = team.split_games_to_teams(games)

# write to new table
dba.create_from_data('team_game_stats', team_games)
dba.insert_rows('team_game_stats', team_games)


dba.create_from_schema('team_season_stats')

season_data = dba.return_data('seasons', modifier="WHERE season >= 2003")
seasons = list(pd.unique(season_data['season']))
seasons.sort()

for season in [2018]:
    where_clause = "WHERE season = {}".format(season)
    team_games = dba.return_data('team_game_stats', modifier=where_clause)
    season_stats = team.summary_by_season(team_games)
    dba.insert_rows('team_season_stats', season_stats)
