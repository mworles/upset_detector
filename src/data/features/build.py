from src.data import Transfer
import tourney
import coach
import pandas as pd

# outcomes from previous tourneys
df = Transfer.return_data('ncaa_results')
ts = tourney.team_success(df)
Transfer.insert_df('tourney_success', ts, at_once=True, create=True)

coaches = Transfer.return_data('coaches')
ts = Transfer.return_data('tourney_success')

# merge coach file with team tourney outcomes file
# outer merge as some coaches will have no tourney games
df = pd.merge(coaches, ts, how='outer', on=['season', 'team_id'])

cs = coach.tourney_success(df)
Transfer.insert_df('coach_success', cs, at_once=True, create=True)
