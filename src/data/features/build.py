from src.data import transfer
import tourney
import coach
import roster
import pandas as pd

# outcomes from previous tourneys
df = transfer.return_data('ncaa_results')
ts = tourney.team_success(df)
transfer.insert_df('tourney_success', ts, at_once=True, create=True)

coaches = transfer.return_data('coaches')
ts = transfer.return_data('tourney_success')

# merge coach file with team tourney outcomes file
# outer merge as some coaches will have no tourney games
df = pd.merge(coaches, ts, how='outer', on=['season', 'team_id'])

cs = coach.tourney_success(df)
transfer.insert_df('coach_success', cs, at_once=True, create=True)

df = roster.run()
transfer.insert_df('roster_features', df at_once=True, create=True)
