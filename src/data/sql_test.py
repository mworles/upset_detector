import pandas as pd
import table_map
import transfer

def make_tables(table_names):
    key = table_map.KEY
    data_path = '../../data/raw/'
    for table_name in tables:
        file_path = '{}{}.csv'.format(data_path, table_name)
        table_data = pd.read_csv(file_path)
        table_data = table_data.rename(columns=key[table_name]['columns'])
        table_name = key[table_name]['new_name']
        dba = transfer.DBAssist()
        dba.create_from_data(table_name, table_data)
        dba.insert_rows(table_name, table_data)
        dba.close()

def player_team_ids():
    dba = transfer.DBAssist()
    team_names = dba.return_data('team_spellings')
    players = dba.return_data('cbb_players')
    merged = pd.merge(players, team_names, left_on='team',
                      right_on='name_spelling', how='inner')
    merged = merged.drop('name_spelling', axis=1)

    dba.create_from_data('cbb_players_teams', merged)
    dba.insert_rows('cbb_players_teams', merged)


tables = ['MTeamConferences', 'Conferences']
#make_tables(table_names)
