from src.data import transfer
import tourney
import coach
import roster
import pandas as pd

dba = transfer.DBAssist()

# outcomes from previous tourneys
df = dba.return_data('ncaa_results')
ts = tourney.team_success(df)
dba.create_insert('tourney_success', ts, at_once=True)

coaches = dba.return_data('coaches')
ts = dba.return_data('tourney_success')

# merge coach file with team tourney outcomes file
# outer merge as some coaches will have no tourney games
df = pd.merge(coaches, ts, how='outer', on=['season', 'team_id'])

cs = coach.tourney_success(df)
dba.create_insert('coach_success', cs, at_once=True)

df = roster.run()
dba.create_insert('roster_features', df, at_once=True)
