import Updater
import datetime
from data import Clean
"""
date = datetime.datetime.now()
# will schedule to run overnight, get results for yesterday's date
date = date - datetime.timedelta(days=1)
date = date.strftime('%Y/%m/%d')

Updater.update_day('2020/02/26')
"""
def update_day_tmp(date):
    # pull all existing games for ratings from current season
    year = float(date.split('/')[0])
    modifier = "WHERE season = %s" % (str(year))
    df = Transfer.return_data('games_for_ratings', modifier=modifier)
    year = Clean.season_from_date(date)
    df = df[df['season'] == year]
    
    # use only games occuring up to date
    df = df[df['date'] <= date]
    
    # compute ratings up to date
    df = Ratings.run_day(df, n_iters=15)
    # insert current ratings to table
    rows = Transfer.dataframe_rows(df)
    Transfer.insert('ratings_at_day', rows)
    
    # insert rows to game_info table for day's games
    mod = """where date = '%s'""" % (date)
    df = Transfer.return_data('game_scores', modifier=mod)
    df = Generate.convert_game_scores(df)
    df = Generate.make_game_info(df)
    rows = Transfer.dataframe_rows(df)
    Transfer.insert('game_info', rows, at_once=False)
    
    # get team location for day's games, insert rows to team_home
    rows = features.Create.game_home(date)
    Transfer.insert('team_home', rows, at_once=False)

    # get team location for upcoming games
    start = Clean.date_plus(date, 1)
    end = Clean.date_plus(start, 5)
    dates = Clean.date_range(start, end)
    rows = Create.game_home(dates)
    Transfer.insert("team_home_current", rows, at_once=True, delete=True)

dates = Clean.date_range("2020/02/25", end_date="2020/02/29")
