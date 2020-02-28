from data import Transfer, Updater
import pandas as pd
import numpy as np

def features_current():
    dba = Transfer.DBAssist()
    dba.connect()
    df = dba.return_df('matchups_current')
    df = df.sort_values('game_id')


    modifier = "WHERE date = (SELECT max(date) from ratings_at_day)"
    ratings = dba.return_df('ratings_at_day', modifier=modifier)
    ratings = ratings.drop(columns=['season', 'date'])
    ratings.columns = [x.replace('team_', '') for x in ratings.columns]
    t1 = ratings.copy()
    t1.columns = ['t1_' + x for x in t1.columns]
    t2 = ratings.copy()
    t2.columns = ['t2_' + x for x in t2.columns]

    mrg = pd.merge(df, t1, left_on='t1_team_id', right_on='t1_id', left_index=True, 
                   how='inner')
    mrg = pd.merge(mrg, t2, left_on='t2_team_id', right_on='t2_id', left_index=True, 
                   how='inner')

    cols_remove = ['game_id', 'date','t1_team_id', 't2_team_id', 't1_id', 't2_id']

    feat = mrg.drop(cols_remove, axis=1).copy()
    return feat
