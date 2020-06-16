import pandas as pd
import numpy as np
from src.data.transfer import DBAssist

def clean_roster(min_season=2002):
    dba = DBAssist()

    # read roster info and player per game stats
    tr = dba.return_data('team_roster')
    pg = dba.return_data('player_pergame')

    # connection no longer needed
    dba.close()

    # merge rows for season, team, and player
    merge_on = ['season', 'team', 'name']
    df = pd.merge(tr, pg, how='inner', left_on=merge_on, right_on=merge_on)

    # drop rows where player_pergame data is missing
    df = df[df['season'] >= min_season]
    
    convert_cols = ['min_pg', 'ast_pg', 'to_pg', 'g', 'pts_pg', 'g_start', 'fgm_pg',
                    'fga_pg', 'fta_pg', 'ftm_pg', 'rbo_pg', 'rbd_pg', 'rb_pg',
                    'stl_pg', 'ast_pg', 'blk_pg', 'pf_pg', 'fg3m_pg']
    
    
    for c in convert_cols:
        df[c] = df[c].astype(float)

    # fill missing minutes for rows that have secondary source available
    df['name'] = df['name'].str.lower()
    df = split_and_fill(df)

    # convert string height to numeric inches
    df['height_num'] = list(map(lambda x: height_inches(x),
                                df['height'].values))

    # clean positon categories
    pos_map = {'C': 'F', 'F': 'F', 'G': 'G', 'F-C': 'F', 'PF': 'F', 'PG': 'G', 
               'SF': 'F', 'SG': 'G'}
    df['position'] = df['position'].map(pos_map)

    df = starter_indicator(df)
    df = experience_indicator(df)

    return df


def split_and_fill(df):
    has = df[df['min_pg'].notnull()].copy()
    tf = df[df['min_pg'].isnull()].copy()
    
    dba = DBAssist()
    # import data from espn per_game table
    ep = dba.return_data('espn_pergame')
    dba.close()

    # create dict to look up by (season, team)
    ref_dict = {}
    
    for name, group in ep.groupby(['season', 'team']):
        ref_dict[name] = group[['name', 'min']].to_dict('list')

    fill_rows = lambda x: fill_minutes(x, ref_dict)
    tf['min_pg'] = tf.apply(fill_rows, axis=1).astype(float)
    all = pd.concat([has, tf])
    all = all.sort_values(['season', 'team'])
    
    # convert minutes to float type
    df['min_pg'] = df['min_pg'].astype(float)
    
    
    # by team, count players missing minutes
    df['mp_null'] = df['min_pg'].isnull().astype(int)
    df['team_mp_null'] = df.groupby(['season', 'team'])['mp_null'].transform(sum)
    # count team players
    df['one'] = 1
    df['team_count'] = df.groupby(['season', 'team'])['one'].transform(sum)
    
    # binary indicator, 1 if team missing minutes for all players
    df['team_mpnone'] = np.where(df['team_mp_null'] == df['team_count'], 1, 0)
    
    # remove temporary cols
    df = df.drop(['mp_null', 'one', 'team_mp_null', 'team_count'], axis=1)
    
    return df


def fill_minutes(row, ref_dict):
    name = row['name']
    team = row['team']
    season = row['season']
    min = row['min_pg']
    if (season, team) in ref_dict.keys():
        ref = ref_dict[(season, team)]
        if name in ref['name']:
            i = ref['name'].index(name)
            min = ref['min'][i]
        else:
            min = None
    else:
        min = None
    
    return min


def height_inches(x):
    '''Returns inches from string height.'''
    try:
        f = int(x.split('-')[0])
        i = int(x.split('-')[1])
        return (f * 12) + i
    except:
        return None


def starter_indicator(df):
    # temporary counter column for computing ordinal ranks
    df['one'] = 1

    # sort by minutes and create minutes rank column
    df = df.sort_values(['season', 'team', 'min_pg'],
                        ascending=[True, True, False])
    df['min_rnk'] = df.groupby(['season', 'team'])['one'].transform('cumsum')

    # sort by games_started and minutes rank, compute combined rank
    df = df.sort_values(['season', 'team', 'g_start', 'min_rnk'],
                        ascending=[True, True, False, True])
    df['gs_rnk'] = df.groupby(['season', 'team'])['one'].transform('cumsum')

    # starters are first 5 of team when sorted by combined gs/minutes rank
    df['starter'] = np.where(df['gs_rnk'] < 6, 'starter', 'bench')
    
    # drop temporary columns
    df = df.drop(['one', 'min_rnk', 'gs_rnk'], axis=1)
    
    return df


def experience_indicator(df):
    # upper case consistency and replace nulls
    df['class'].fillna('', inplace=True)
    df['class'] = df['class'].str.upper()

    # numeric experience from class
    expdict = {'SR': 3, 'JR': 2, 'SO': 1, 'FR': 0}
    df['yrs_exp'] = df['class'].map(expdict)
    
    return df
