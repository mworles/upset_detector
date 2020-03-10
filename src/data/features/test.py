from src.data import Transfer, Clean
import pandas as pd
import team


def sum_stats(gb):
    cols_to_sum = ['team_win', 'team_loss']
    tgb_sum = gb[cols_to_sum].sum()
    tgb_sum.columns = ['wins', 'losses']
    tgb_sum = tgb_sum.reset_index()
    return tgb_sum

def mean_stats(df):
    gb = df.groupby('team_id')
    # make list of columns for mean aggregation
    aggmean = df.columns
    not_mean = ['team_win', 'team_loss', 'team_id', 'opp_id', 'date']
    aggmean = [x for x in aggmean if x not in not_mean]

    tgb = df.groupby('team_id')
    ts_mean = tgb[aggmean].mean()
    ts_mean = ts_mean.round(4)
    ts_mean = ts_mean.rename(columns={'team_score': 'ppg',
                                      'opp_score': 'ppg_d'})
    ts_mean = ts_mean.reset_index()
    return ts_mean

def pct_last(group, n=5):
    #Compute win percentage in last n games.
    wins = group.iloc[-n:].sum()
    return wins / float(n)

def streak(y):
    #Compute number of consecutive events.
    streak = y * (y.groupby((y != y.shift()).cumsum()).cumcount() + 1)
    return int(streak[-1:])

def prep_season(season, subset=None):
    mod = """WHERE season = %s""" % (season)
    if subset == 'ncaa':
        ncaa = Transfer.return_data('ncaa_results', mod)
        first_day = ncaa['daynum'].min()
        mod = mod + """AND daynum >= %s""" % (first_day)

    df = Transfer.return_data('stats_by_team', mod)
    
    start_cols = df.columns.tolist()
    
    # add computed stats to data
    df = team.compute_game_stats(df)
    
    # keep computed stats and select columns
    new_cols = [x for x in list(df.columns) if x not in start_cols]
    new_cols.remove('pos')
    keep_cols = ['season', 'daynum', 'team_id', 'team_score', 'opp_id',
                 'opp_score']
    keep_cols.extend(new_cols)
    df = df[keep_cols]
    
    df = pd.merge(df, dfs, how='inner', left_on='season', right_on='season')
    df['date'] = df.apply(Clean.game_date, axis=1)
    
    
    dfy = df.drop(['season', 'daynum', 'dayzero'], axis=1)
    
    return dfy

def prep_date(dfy, date):
    dfyd = df[df['date'] < date]
    dfyd = dfyd.sort_values(['team_id', 'date'])
    
    tgb_mean = mean_stats(dfyd)
    
    gb = dfyd.groupby('team_id')
    tgb_sum = sum_stats(gb)

    tgb_std = gb['scrmarg'].std()
    tgb_std = tgb_std.rename('scrmargsd').round(2)
    tgb_std = tgb_std.reset_index()
    
    recent_list = []
    
    recent_list.append(gb['team_win'].apply(streak).rename('wstreak'))
    recent_list.append(gb['team_loss'].apply(streak).rename('lstreak'))

    # need to remove teams with < 5/10 games
    game_count = gb['team_win'].count().reset_index()
    have_five = game_count[game_count['team_win'] >=5]['team_id'].values
    have_ten = game_count[game_count['team_win'] >=10]['team_id'].values
    df5 = dfyd[dfyd['team_id'].isin(have_five)]
    df10 = dfyd[dfyd['team_id'].isin(have_ten)]

    gb5 = df5.groupby('team_id')
    gb10 = df10.groupby('team_id')
    
    last_5 = lambda x: pct_last(x, n=5)
    pct_last_5 = gb5['team_win'].apply(last_5).rename('wpctlast5')
    
    last_10 = lambda x: pct_last(x, n=10)
    pct_last_10 = gb10['team_win'].apply(last_10).rename('wpctlast10')

    tgb_rec = pd.merge(pct_last_5, pct_last_10, left_on='team_id',
                      right_on='team_id', how='outer')

    mrg1 = pd.merge(tgb_sum, tgb_mean, how='outer',
                    left_on='team_id', right_on='team_id')
    mrg2 = pd.merge(mrg1, tgb_std, how='outer', left_on='team_id',
                    right_on='team_id')
    mrg3 = pd.merge(mrg2, tgb_rec, how='outer',
                    left_on='team_id', right_on='team_id')

    mrg3['winpct'] = mrg3.wins / (mrg3.wins + mrg3.losses)
    
    return mrg3

#Transfer.create_from_schema('stats_by_date', '../schema.json')

mod = """WHERE season >= 2003"""
dfs = Transfer.return_data('seasons', mod)
dfs = dfs[['season', 'dayzero']]
seasons = list(set(dfs['season'].values))
seasons.sort()

for season in seasons:
    df = prep_season(season, subset='ncaa')
    dates = list(set(df['date']))
    dates.sort()

    for date in dates[1:]:
        
        df_date = prep_date(df, date)
        
        df_date['date'] = date
        df_date['season'] = season
                        
        Transfer.insert_df('stats_by_date', df_date, at_once=True)
