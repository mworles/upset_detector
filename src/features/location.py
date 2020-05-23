""" location

A module for transforming and linking data with the physical location of
events, places, or teams. Uses latittude and longitude for geographical
coordinates.

Functions
---------
run
    Return dataframe with game identifer and geographical coordinates.

home_games
    Return dataframe with coordinates for games at a team home site.

neutral_games
    Return dataframe with coordinates for games at a neutral site.

locate_item
    Return the geographical coordinates of an item from a dictionary.

city_coordinates
    Return dict mapping cities to geographical coordinates.

team_coordinates
    Return dict mapping teams to geographical coordinates.

gym_schedule_coordinates
    Return dict mapping gyms to geographical coordinates.

match_gym
    Return a fuzzy string match for a given gym name.

gym_city_coordinates
    Return dict mapping gyms from gym/city data to geographical coordinates.

transform_schedule
    Return team schedule dataframe after cleaning tranformations.

convert_schedule_date
    Return date from team schedule converted to the project's date format.

schedule_team_ids
    Return dataframe pairing team numeric identifers to team names.

game_distances
    Return dataframe with team distance to game.

travel_distance
    Return integer distance between a pair of geographical coordinates.

update_teams
    Return dataframe with additional team geographical coordinates added.

"""
import re
from datetime import datetime
import pandas as pd
import numpy as np
from geopy.distance import great_circle
from src.data.transfer import DBAssist
from src.data import transfer
from src.data import clean
from src.data import generate

def run(modifier=None):
    """
    Return dataframe with unique game identifers and location of games.

    Parameters
    ----------
    modifier : str
        Modifier for MySQL query to pull game data from game_info table.

    Returns
    -------
    df : pandas DataFrame
        Contains game id, latittude, and longitude of game.

    """
    # import games data, contains game and team identifiers
    dba = DBAssist()
    df = dba.return_data('game_info', modifier=modifier)
    th = dba.return_data('team_home')
    # connection no longer needed, close
    dba.close()

    # to add data on where game was hosted
    th = home_games(th, df['game_id'].values)
    
    df = pd.merge(df, th, left_on='game_id', right_on='game_id', how='left')

    # add column indicating host is neutral or not
    df['neutral'] = np.where(df['home_id'].isnull(), 1, 0)

    # select non-neutral games to get location of home team
    dfh = df[df['neutral'] == 0].copy()

    # create subset of all neutral games
    dfn = df[df['neutral'] == 1].copy().drop(['game_loc'], axis=1)
    neutral = dfn['game_id'].values

    # import available locations for neutral games
    # contains game_id and location coordinates
    games = neutral_games(neutral)

    # right merge to neutral games
    dfn = pd.merge(dfn, games, how='right', left_on='game_id',
                   right_on='game_id')

    # combine non-neutral and neutral games
    df = pd.concat([dfh, dfn])

    # split location to longitude and lattitude for new table
    df['latitude'] = map(lambda x: round(x[0], 3), df['game_loc'].values)
    df['longitude'] = map(lambda x: round(x[1], 3), df['game_loc'].values)
    df = df[['game_id', 'latitude', 'longitude']]

    return df


def home_games(df, game_id):
    """
    Return dataframe with identifiers and coordinates for games with a 
    home team.

    Parameters
    ----------
    game_id : list or array
        List of game identifiers to select home games from.

    Returns
    -------
    df : DataFrame
        Contains game id, home team id, and coordinate location of game.

    """
    df = df[df['game_id'].isin(game_id)]

    # identify home team for game locations
    df = df[df['home'] == 1]
    df = df.drop(['date', 'home'], axis=1)
    df = df.rename(columns={'team_id': 'home_id'})

    team_map = team_coordinates(df['home_id'].values)
    home = df['home_id'].values
    df['game_loc'] = map(lambda x: locate_item(x, team_map), home)
    df = df[['game_id', 'home_id', 'game_loc']]

    return df


def neutral_games(neutral):
    """
    Return dataframe with identifiers and coordinates for games played at
    a neutral site.
    
    Imports game locations from different sources and contains sequence of 
    steps to transform and link games to locations. 
    
    Parameters
    ----------
    neutral : list or array
        List of game identifiers to select games from.

    Returns
    -------
    all : DataFrame
        Contains game id and coordinate location of game.

    """
    # import and merge game cities and cities data, available after 2010
    dba = DBAssist()
    games = dba.return_data('game_cities')
    cities = dba.return_data('cities')

    df = pd.merge(games, cities, how='inner', left_on='city_id',
                  right_on='city_id')

    # create game id and keep relevant games
    df = clean.date_from_daynum(df)
    df = clean.order_team_id(df, ['wteam', 'lteam'])
    df = clean.make_game_id(df)
    df = df[df['game_id'].isin(neutral)]

    # import map of city coordinates,
    city_map = city_coordinates(full_state=False)

    # create array of (city,state) tuples, locate coordinates
    city_state = zip(df['city'].values, df['state'].values)
    df['game_loc'] = map(lambda x: locate_item(x, city_map), city_state)

    # import and combine tourney games prior to 2010
    tg = dba.return_data('tourney_geog', modifier='WHERE season < 2010')

    # add unique game id from teams and date
    tg = clean.date_from_daynum(tg)
    tg = clean.order_team_id(tg, ['wteam', 'lteam'])
    tg = clean.make_game_id(tg)
    tg['game_loc'] = zip(tg['latitude'].values, tg['longitude'].values)

    # import and clean games with gyms from scraped schedule
    mod = 'WHERE season >= 2003 AND season <= 2009'
    sg = dba.return_data('cbb_schedule', modifier=mod)
    sg = transform_schedule(sg)

    # remove tourney games already obtained above
    sg = sg[~sg['game_id'].isin(tg['game_id'].values)]

    # create gym location map and set game locations
    gym_map = gym_schedule_coordinates(sg)
    sg['game_loc'] = map(lambda x: locate_item(x, gym_map), sg['gym'].values)

    # combine all games, keep games with valid locations
    all = pd.concat([df, tg, sg], sort=False)
    all = all[all['game_loc'].notnull()]
    all = all[['game_id', 'game_loc']]
    
    dba.close()

    return all


def locate_item(item, item_map):
    """
    Return geographical coordinates of an item from a dictionary.

    Parameters
    ----------
    item : str or tuple of str
        The item for which to find the geographical coordinates.
    item_map : dict
        Contains items as keys, coordinates as values.

    Returns
    -------
    result : tuple
        Tuple of floats contining lattitude and longitude.

    """
    try:
        result = item_map[item]
    except KeyError:
        result = None

    return result


def city_coordinates(full_state=True):
    """
    Return a dict mapping cities to lattitude and longitude.

    Parameters
    ----------
    full_state : bool, default True
        Use the full state name in the unique key for each city.

    Returns
    -------
    city_dict : dict
        Keys are tuples (city, state), values are tuples (lattitude,longitude).

    """
    # import and combine city and state data
    dba = DBAssist()
    usc = dba.return_data('us_cities')
    uss = dba.return_data('us_states')
    dba.close()

    uss = uss.rename(columns={'ID': 'ID_STATE'})
    df = pd.merge(usc, uss, left_on='ID_STATE', right_on='ID_STATE',
                  how='inner')

    # create state_map for supplemental cities
    state_map = df[['STATE_CODE', 'STATE_NAME']].drop_duplicates().copy()
    state_map = state_map.set_index('STATE_CODE')
    state_map = state_map['STATE_NAME'].to_dict()
    
    # import and transform supplemental cities
    sc = manual_cities(state_map)

    # combine all cities into one set
    ct = pd.concat([df, sc], sort=False)

    # replace strings for consistency across data sources
    ct['CITY'] = ct['CITY'].str.replace('Saint', 'St.')

    # specify which state identifier to use
    if full_state is True:
        state = 'STATE_NAME'
    else:
        state = 'STATE_CODE'

    # keep just the columns needed for the map
    ct = ct[['CITY', state, 'LATITUDE', 'LONGITUDE']]
    ct = ct.drop_duplicates(subset=['CITY', state])

    # create dict with tuples as keys (city, state) and values (lat, lng)
    ct['city_state'] = zip(ct['CITY'].values, ct[state].values)
    ct['lat_lng'] = zip(ct['LATITUDE'].values, ct['LONGITUDE'].values)
    ct = ct.set_index('city_state')
    city_dict = ct['lat_lng'].to_dict()

    return city_dict


def manual_cities(state_map):
    """
    Return dataframe containing data for supplemental cities to add to
    original US cities table.

    Parameters
    ----------
    state_map : dict
        Dict mapping state codes to full state names.

    Returns
    -------
    df : DataFrame
        Contains city, state/country, and location for supplemental cities.

    """
    # import data for supplemental cities
    dba = DBAssist()
    df = dba.return_data('cities_manual')
    dba.close()

    df = df.rename(columns={'city': 'CITY',
                            'state': 'STATE_CODE',
                            'lat': 'LATITUDE',
                            'lng': 'LONGITUDE'})

    # assign "state" values for foreign countries
    map_update = {'VI': 'Virgin Islands',
                  'BA': 'Bahamas',
                  'MX': 'Mexico',
                  'CI': 'Cayman Islands',
                  'JA': 'Jamaica',
                  'IR': 'Ireland'}
    state_map.update(map_update)
    df['STATE_NAME'] = df['STATE_CODE'].map(state_map)

    return df


def team_coordinates(team_id):
    """
    Return a dict mapping teams to geographical coordinates.
    
    Parameters
    ----------
    team_id : list or array
        List of team identifiers to select home games from.

    Returns
    -------
    team_map : dict
        Keys are integer team ids, values are tuples (lattitude,longitude).

    """
    # import team geography data, select teams
    dba = DBAssist()
    df = dba.return_data('team_geog')
    dba.close()

    df = df[df['team_id'].isin(team_id)]
    
    # create map with (lattitude, longitude) tuples as values
    df['lat_lng'] = zip(df['latitude'].values, df['longitude'].values)
    df = df.set_index('team_id')
    team_map = df['lat_lng'].to_dict()

    return team_map


def gym_schedule_coordinates(df):
    """
    Return a dict mapping gyms from the input schedule data to geographical
    coordinates of gyms from a separate external source. Coordinates are linked
    using either exact match or fuzzy string match to gym name. 

    Parameters
    ----------
    df : DataFrame
        Contains games from team schedules with gym names.

    Returns
    -------
    gym_dict : dict
        Keys are string gym names, values are tuples (lattitude,longitude).

    """
    # get dict with keys as gym names and values as (city_state) locations
    dba = DBAssist()
    gg = dba.return_data('game_gym')
    dba.close()

    gym_map = gym_city_coordinates(gg)

    # isolate the unique gym names, only need one row per unique gym
    df = df.drop_duplicates(subset=['gym'])

    # isolate gyms with and without a match in gym_dict
    in_dict = df['gym'].isin(gym_map.keys())
    df_in = df[in_dict].copy()
    df_out = df[~in_dict].copy()
    
    #  add fuzzy-matched gym names
    options = gym_map.keys()
    names = df_out['gym'].values
    df_out['fuzz'] = map(lambda x: match_gym(x, options), names)

    # combine merged and fuzzy matched gym location data
    gyms = pd.concat([df_in, df_out], sort=False)

    # create array of exact match or fuzzy match if no exact exists
    gym_orig, gym_fuzz = gyms['gym'].values, gyms['fuzz'].values
    locate = np.where(gyms['fuzz'].isnull(), gym_orig, gym_fuzz)
    
    # get coordinates for each gym in locate
    gyms['gym_loc'] = map(lambda x: locate_item(x, gym_map), locate)
    
    # create and return gym: location map
    gyms = gyms.set_index('gym')
    gym_dict = gyms['gym_loc'].to_dict()

    return gym_dict


def match_gym(name, options):
    """
    Return the closest match to a gym name from a list of options using a
    fuzzy string matching algorithm.

    Parameters
    ----------
    name : str
        The name of the gym to find a fuzzy match for.
    options: list
        List of strings with options to search over.

    Returns
    -------
    result : str
        The string with the highest match score.

    """
    try:
        result = clean.fuzzy_match(name, options, cutoff=90)
    # to catch strings with remaining unicode
    except TypeError:
        result = None

    return result


def gym_city_coordinates(df):
    """
    Return a dict mapping gym names from external source of gym cities
    to geographical coordinates.

    A sequence of steps cleans the gym and city values prior to creating the 
    gym map.

    Parameters
    ----------
    df : DataFrame
        Data containing web-scraped gym names, cities, and states.

    Returns
    -------
    gym_map : dict
        Keys are string gym names, values are tuples (lattitude,longitude).

    """
    # only need one row per unique gym location
    df = df[['gym', 'city', 'state']].drop_duplicates()
    df['city'] = df['city'].str.replace('-', ' ')
    df['city'] = df['city'].str.replace('Saint', 'St.')
    df['gym'] = df['gym'].str.lower()
    df = df.apply(lambda x: x.str.strip())
    
    # remove specific irrelevant duplicates
    na1 = ((df['gym'] == 'wells fargo arena') & (df['city'] == 'Anchorage'))
    df = df[~na1]
    na2 = ((df['gym'] == 'toyota center') & (df['city'] == 'Kennewick'))
    df = df[~na2]

    # after cleaning some duplicates remain, remove them
    df = df[~df['gym'].duplicated()]
    df = df.set_index('gym')
    df['city_state'] = zip(df['city'].values, df['state'].values)

    # dict of coordinates for cities
    city_map = city_coordinates(full_state=True)
    
    # create dict mapping gyms to city coordinates
    city_state = df['city_state'].values
    df['game_loc'] = map(lambda x: locate_item(x, city_map), city_state)
    gym_dict = df['game_loc'].to_dict()

    # update dict with manual gym locations
    dba = DBAssist()
    gm = dba.return_data('gym_manual', modifier=None)
    dba.close()

    gm['game_loc'] = zip(gm['lat'].values, gm['lng'].values)
    gm = gm.set_index('gym')['game_loc'].to_dict()
    gym_dict.update(gm)

    return gym_dict


def transform_schedule(df):
    """
    Return dataframe with unique game identifier and gym name for games 
    scraped from team schedules.
    
    Sequence of data transformations create the project game
    identifier and prepare gym names for merging with gym city/state data.

    Parameters
    ----------
    df : DataFrame
        Contains scraped team schedule data in 'cbb_schedule' table.

    Returns
    -------
    dft : DataFrame
        Transformed data with game identifier and gym name.

    """    
    # only use neutral site games with gym values
    dft = df[df['location'] == 'N']
    dft = dft[dft['gym'].notnull()]

    # clean gym values for better alignment with gym city data
    def clean_gym(name):
        new_name = name.lower()
        new_name = re.sub(r"\(.*\)", "", new_name).rstrip()
        if 'agrelot' in new_name.split(' '):
            new_name = 'josa miguel agrelot coliseum'
        return new_name
    
    dft['gym'] = map(clean_gym, dft['gym'].values)
    # need standard date to create game id
    dft['date'] = map(convert_schedule_date, dft.date.values)
    # clean up opponent names by removing team ranks
    dft['opponent'] = map(lambda x: re.sub(r"\(\d*\)", "", x).rstrip(),
                         dft['opponent'].values)

    # obtain team numeric ids and game id for each game
    dft = schedule_team_ids(dft)
    df = clean.order_team_id(df, 'team_id', 'opp_id')
    df = clean.make_game_id(df)
    
    # isolate unique gym names for matching to gym locations
    dft = dft[['game_id', 'gym']]

    # each game has 2 rows (1 for each team), keep one
    dft = dft.drop_duplicates(subset='game_id')

    return dft


def convert_schedule_date(raw_date):
    """Returns date in scraped team schedule converted to the project 
    standard date format.

    Parameters
    ----------
    raw_date : str
        The original date string scraped from team schedule.

    Returns
    -------
    date : str
        Date converted to the project standard string format.

    """
    # remove irrelevant day of week from string
    datestr = ' '.join([x.strip() for x in raw_date.split(',')[1:]])
    dt = datetime.strptime(datestr, "%b %d %Y")
    date = dt.strftime("%Y/%m/%d")

    return date


def schedule_team_ids(df):
    """
    Return schedule dataframe with team numeric identifers added.
    
    Parameters
    ----------
    df : DataFrame
        Contains data scraped from team schedule website. Teams are identified
        by string names in columns 'team' and opponent'.

    Returns
    -------
    df : DataFrame
        Contains team numeric ids for both teams in the game.

    """
    dba = DBAssist()
    tk = dba.return_data('team_key')
    tk = tk[['team_id', 'team_ss']].copy()
    tk = tk.drop_duplicates()

    # import and merge the team names from team schedule links
    # schedule data uses names from both team_key and team_sched
    ts = dba.return_data('team_sched')
    ts['team_ss'] = ts['team_ss'].replace('Cal State Long Beach',
                                          'Long Beach State')
    
    # connection no longer needed
    dba.close()
    
    # inner merge because only need ids for teams in team_sched
    tk = pd.merge(ts, tk, left_on='team_ss', right_on='team_ss', how='inner')
    tk['team_id'] = tk['team_id'].astype(int)
    
    tkm = tk[['team_id', 'team_sched']].copy()
    tkm = tkm.rename(columns={'team_sched': 'team'})
    df = pd.merge(df, tkm, left_on='team', right_on='team', how='inner')

    # merge numeric ids for opponents
    tkm = tk[['team_id', 'team_ss']].copy()
    tkm = tkm.rename(columns={'team_ss': 'opponent', 'team_id': 'opp_id'})
    df = pd.merge(df, tkm, left_on='opponent', right_on='opponent',
                  how='inner')

    return df


def game_distances(df, team_map):
    """
    Return dataframe with distance to game computed for both teams.

    Parameters
    ----------
    df : DataFrame
        Contains 'game_loc' column with coordinates of game location and team
        numeric identifiers.
    team_map : dict
        Dict mapping team numeric identifers to geographical coordinates.

    Returns
    -------
    df : DataFrame
        Contains distance (in miles) to game in separate column for each team.

    """

    # get locations for both teams in game
    t1 = map(lambda x: locate_item(x, team_map), df['t1_team_id'].values)
    t2 = map(lambda x: locate_item(x, team_map), df['t2_team_id'].values)

    # use locations to set travel distance for both teams
    df['t1_dist'] = map(travel_distance, zip(df['game_loc'].values, t1))
    df['t2_dist'] = map(travel_distance, zip(df['game_loc'].values, t2))

    return df


def travel_distance(point_pair):
    """
    Return integer distance between a pair of geographical coordinates.

    Parameters
    ----------
    point_pair : tuple
        Tuple of 2 tuples, each containing the (lattitude, longitude) of the
        points to compute the distance between.

    Returns
    -------
    result : int
        The distance (in miles) between the pair of points.

    """
    distance = int(great_circle(point_pair[0], point_pair[1]).miles)
    return distance


def update_teams(df, team_cities):
    """
    Return dataframe with geographical coordinates of new teams to add to
    original team location data.

    Parameters
    ----------
    df : DataFrame
        The original raw team location data. Contains team id number and
        geographical coordinates of team.
    team_cities : list
        Valid list element is 2-item list with team id and (city, state).

    Returns
    -------
    df_add : DataFrame
        Team id number and geographical coordinates of teams to add.

    """
    # obtain list of team ids present in existing table
    teams_have = df['team_id'].values

    # keep teams to add if not in existing list
    teams_add = [x for x in team_cities if x[0] not in teams_have]

    if len(teams_add) > 0:
        # import map of cities to get coordinates from
        cm = city_coordinates()
        data_add = []

        # obtain id, lattitude, longitude for each team
        for row in teams_add:
            lat, lng = cm[row[1]][0], cm[row[1]][1]
            team_data = [row[0], lat, lng]
            data_add.append(team_data)

        # insert new data to table
        df_add = pd.DataFrame(data_add, columns=list(df.columns))

    return df_add
