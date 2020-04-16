""" location

A module for handling and transforming data pertaining to the physical location
of events, places, or teams.

Functions
---------
run
    Return data with game identifer, latitude, and longitude.

city_locations
    Return dict mapping cities to lattitude and longitude.

home_locations
    Return dataframe with game identifer, home team, and location.

locate_item
    Return item location tuple from a given dict.

team_locations
    Return dict mapping teams to location.

neutral_locations
    Return dataframe with game identifier and location of neutral games.

transform_schedule
    Return game identifiers and gym names from scraped team schedule data.

gym_locations
    Return dict with locations for the gyms in team schedule data.

gym_coordinates
    Return dict with location of gyms collected from gym, city, state data.

match_gyms
    Returns a fuzzy string match for a given gym name.

convert_schedule_date
    Returns date converted to the project standard format.

schedule_team_ids
    Returns dataframe with team numeric identifers added to raw schedule data.

schedule_team_key
    Returns a dataframe with unique keys for linking schedule teams to numeric
    identifiers.

game_distances
    Returns dataframe with distances to game for both teams in the matchup.

travel_distance
    Returns integer distance (in miles) between two locations.

update_teams
    Inserts geographical location for new teams not present in the raw team
    geographical location file.

"""

from src.data import Transfer
from src.data import Clean
from src.data import Generate
import pandas as pd
import numpy as np
import re
from datetime import datetime
from geopy.distance import great_circle


def run(modifier=None):
    """Return data with game identifer, latitude, and longitude."""
    # import games data with host team and site details
    df = Transfer.return_data('game_info', modifier=modifier)
    df = df.drop(['t1_score', 't2_score', 't1_win', 't1_marg'], axis=1)

    # to add data on where game was hosted
    th = home_locations(df['game_id'].values)
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
    games = neutral_locations(neutral)

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


def city_locations(full_state=True):
    """Obtain dict mapping cities to their geographical coordinates.

    Parameters
    ----------
    full_state : bool, default True
        Use the full state name in the unique key for each city.

    Returns
    -------
    city_dict : dict
        Keys as (city, state) tuples, values as dict with coordinate keys.

    """
    # import and combine city and state data
    usc = Transfer.return_data('us_cities')
    uss = Transfer.return_data('us_states')
    uss = uss.rename(columns={'ID': 'ID_STATE'})
    ct = pd.merge(usc, uss, left_on='ID_STATE', right_on='ID_STATE',
                  how='inner')

    # add supplemental cities
    sc = Transfer.return_data('cities_manual')
    sc = sc.rename(columns={'city': 'CITY',
                            'state': 'STATE_CODE',
                            'lat': 'LATITUDE',
                            'lng': 'LONGITUDE'})

    # create dict for full state name from state code
    state_map = ct[['STATE_CODE', 'STATE_NAME']].drop_duplicates().copy()
    state_map = state_map.set_index('STATE_CODE')
    state_map = state_map['STATE_NAME'].to_dict()

    # assign "state" values for foreign countries
    map_update = {'VI': 'Virgin Islands',
                  'BA': 'Bahamas',
                  'MX': 'Mexico',
                  'CI': 'Cayman Islands',
                  'JA': 'Jamaica',
                  'IR': 'Ireland'}
    state_map.update(map_update)
    sc['STATE_NAME'] = sc['STATE_CODE'].map(state_map)

    # combine all cities into one set
    ct = pd.concat([ct, sc], sort=False)

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


def home_locations(game_id):
    """Import home team identifier data.

    Parameters
    ----------
    modifier : str, optional, default None
        MYSQL string to specify subsets of rows (e.g., 'WHERE year > 2000')

    Returns
    -------
    df : DataFrame
        Contains game id and home team id.

    """
    # import team_home data
    df = Transfer.return_data('team_home')
    df = df[df['game_id'].isin(game_id)]

    # identify home team for game locations
    df = df[df['home'] == 1]
    df = df.drop(['date', 'home'], axis=1)
    df = df.rename(columns={'team_id': 'home_id'})

    team_map = team_locations()
    home = df['home_id'].values
    df['game_loc'] = map(lambda x: locate_item(x, team_map), home)
    df = df[['game_id', 'home_id', 'game_loc']]

    return df


def locate_item(item, item_map):
    """Return the geographical coordinates of an item.

    Parameters
    ----------
    item : str or tuple of str
        The item for which to find the geographical coordinates.
    item_map : dict
        Contains items as keys, coordinates as values.

    Returns
    -------
    result : tuple
        Tuple of numbers contining lattitude and longitude.

    """
    try:
        result = item_map[item]
    except KeyError:
        result = None

    return result


def team_locations():
    """Return dictionary map of locations for team numeric identifiers."""
    # import team geography data
    df = Transfer.return_data('team_geog')

    # create map with (lattitude, longitude) tuples as values
    df['lat_lng'] = zip(df['latitude'].values, df['longitude'].values)
    df = df.set_index('team_id')
    tm = df['lat_lng'].to_dict()

    return tm


def neutral_locations(neutral):
    # import and merge game cities and cities data, available after 2010
    games = Transfer.return_data('game_cities')
    cities = Transfer.return_data('cities')
    df = pd.merge(games, cities, how='inner', left_on='city_id',
                  right_on='city_id')

    # create game id and keep relevant games
    df = Generate.make_game_id(df, ['wteam', 'lteam'])
    df = df[df['game_id'].isin(neutral)]

    # import map of city coordinates,
    city_map = city_locations(full_state=False)

    # create array of (city,state) tuples, locate coordinates
    city_state = zip(df['city'].values, df['state'].values)
    df['game_loc'] = map(lambda x: locate_item(x, city_map), city_state)

    # import and combine tourney games prior to 2010
    tg = Transfer.return_data('tourney_geog', modifier='WHERE season < 2010')

    # add unique game id from teams and date
    tg = Generate.make_game_id(tg, ['wteam', 'lteam'])
    tg['game_loc'] = zip(tg['latitude'].values, tg['longitude'].values)

    # import and clean games with gyms from scraped schedule
    mod = 'WHERE season >= 2003 AND season <= 2009'
    sg = Transfer.return_data('cbb_schedule', modifier=mod)
    sg = transform_schedule(sg)

    # remove tourney games already obtained above
    sg = sg[~sg['game_id'].isin(tg['game_id'].values)]

    # create gym location map and set game locations
    gym_map = gym_locations(sg)
    sg['game_loc'] = map(lambda x: locate_item(x, gym_map), sg['gym'].values)

    # combine all games, keep games with valid locations
    all = pd.concat([df, tg, sg], sort=False)
    all = all[all['game_loc'].notnull()]
    all = all[['game_id', 'game_loc']]

    return all


def transform_schedule(df):
    # only use neutral site games with gym values
    df = df[df['location'] == 'N']
    df = df[df['gym'].notnull()]

    # clean gym values for bettter alignment with gym city data
    def clean_gym(name):
        new_name = name.lower()
        new_name = re.sub(r"\(.*\)", "", new_name).rstrip()
        return new_name

    df['gym'] = map(clean_gym, df['gym'].values)
    # convert date to the project's standard date format
    df['date'] = map(convert_schedule_date, df.date.values)
    # clean up opponent names by removing team ranks
    df['opponent'] = map(lambda x: re.sub(r"\(\d*\)", "", x).rstrip(),
                         df['opponent'].values)

    # obtain team numeric ids and game id for each game
    df = schedule_team_ids(df)
    df = Generate.make_game_id(df, ['team_id', 'opp_id'], convert_date=False)

    # isolate unique gym names for matching to gym locations
    df = df[['game_id', 'gym']]

    # each game has 2 rows (1 for each team), keep one
    df = df.drop_duplicates(subset='game_id')

    return df


def gym_locations(df):

    df = df.drop_duplicates(subset=['gym'])

    # dict with keys as gym names and values as (city_state) locations
    gym_map = gym_coordinates()

    # isolate gyms with and without a match in gym_dict
    in_dict = df['gym'].isin(gym_map.keys())
    df_in = df[in_dict].copy()
    df_out = df[~in_dict].copy()

    # use match_gyms to add fuzzy-matched gym names
    options = gym_map.keys()
    names = df_out['gym'].values
    df_out['fuzz'] = map(lambda x: match_gym(x, options), names)

    # combine merged and fuzzy matched gym location data
    gyms = pd.concat([df_in, df_out], sort=False)

    # comment
    g, f = gyms['gym'].values, gyms['fuzz'].values
    locate = np.where(gyms['fuzz'].isnull(), g, f)
    gyms['gym_loc'] = map(lambda x: locate_item(x, gym_map), locate)

    gyms = gyms.set_index('gym')
    gym_dict = gyms['gym_loc'].to_dict()

    return gym_dict


def gym_coordinates(modifier=None):
    df = Transfer.return_data('game_gym', modifier=modifier)
    df = df[['gym', 'city', 'state']].drop_duplicates()
    df['city'] = df['city'].str.replace('-', ' ')
    df['city'] = df['city'].str.replace('Saint', 'St.')
    df['gym'] = df['gym'].str.lower()
    df = df.apply(lambda x: x.str.strip())
    df = df[~df['gym'].duplicated()]
    df = df.set_index('gym')
    df['city_state'] = zip(df['city'].values, df['state'].values)

    # dict of coordinates for cities
    city_map = city_locations(full_state=True)

    city_state = df['city_state'].values
    df['game_loc'] = map(lambda x: locate_item(x, city_map), city_state)

    gym_dict = df['game_loc'].to_dict()

    # update dict with manual gym locations
    gm = Transfer.return_data('gym_manual', modifier=None)
    gm['game_loc'] = zip(gm['lat'].values, gm['lng'].values)
    gm = gm.set_index('gym')['game_loc'].to_dict()
    gym_dict.update(gm)

    return gym_dict


# function for fuzzy matching gym names
def match_gym(name, options):
    try:
        result = Clean.fuzzy_match(name, options, cutoff=90)
    except TypeError:
        if 'agrelot' in name.split(' '):
            result = 'josa miguel agrelot coliseum'
        else:
            result = None
    return result


def convert_schedule_date(raw_date):
    """Converts raw team schedule date to standard format for the project."""
    # remove irrelevant day of week from string
    datestr = ' '.join([x.strip() for x in raw_date.split(',')[1:]])
    dt = datetime.strptime(datestr, "%b %d %Y")
    date = dt.strftime("%Y/%m/%d")

    return date


def schedule_team_ids(df):
    # merge in numeric team ids for teams
    tk = schedule_team_key()
    tkm = tk[['team_id', 'team_sched']].copy()
    tkm = tkm.rename(columns={'team_sched': 'team'})
    df = pd.merge(df, tkm, left_on='team', right_on='team', how='inner')

    # merge numeric ids for opponents
    tkm = tk[['team_id', 'team_ss']].copy()
    tkm = tkm.rename(columns={'team_ss': 'opponent', 'team_id': 'opp_id'})
    df = pd.merge(df, tkm, left_on='opponent', right_on='opponent',
                  how='inner')

    keep_cols = ['season', 'date', 'location', 'gym', 'team_id', 'opp_id']
    df = df[keep_cols]

    return df


def schedule_team_key():
    # import team key data to link team ids to applicable cbb team names
    tk = Transfer.return_data('team_key')
    tk = tk[['team_id', 'team_ss']].copy()
    tk = tk.drop_duplicates()

    # import and merge the team names from team schedule links
    # schedule data uses names from both team_key and team_sched
    ts = Transfer.return_data('team_sched')
    ts['team_ss'] = ts['team_ss'].replace('Cal State Long Beach',
                                          'Long Beach State')

    # inner merge because only need ids for teams in team_sched
    tk = pd.merge(ts, tk, left_on='team_ss', right_on='team_ss', how='inner')
    tk['team_id'] = tk['team_id'].astype(int)

    return tk


def game_distances(df, team_map):
    # get locations for both teams in game
    t1 = map(lambda x: locate_item(x, team_map), df['t1_team_id'].values)
    t2 = map(lambda x: locate_item(x, team_map), df['t2_team_id'].values)

    # use locations to set travel distance for both teams
    df['t1_dist'] = map(travel_distance, zip(df['game_loc'].values, t1))
    df['t2_dist'] = map(travel_distance, zip(df['game_loc'].values, t2))

    return df


def travel_distance(point_pair):
    """Return the distance between a pair of geographical coordinates.

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


def update_teams(team_cities):
    """Update team location table with any new teams.

    Parameters
    ----------
    team_cities : list
        Valid list element is 2-item list with team id and (city, state).

    """
    # obtain list of team ids present in existing table
    df = Transfer.return_data('team_geog')
    teams_have = df['team_id'].values

    # keep teams to add if not in existing list
    teams_add = [x for x in team_cities if x[0] not in teams_have]

    if len(teams_add) > 0:
        # import map of cities to get coordinates from
        cm = city_locations()
        data_add = []

        # obtain id, lattitude, longitude for each team
        for row in teams_add:
            lat, lng = cm[row[1]][0], cm[row[1]][1]
            team_data = [row[0], lat, lng]
            data_add.append(team_data)

        # insert new data to table
        df_add = pd.DataFrame(data_add, columns=list(df.columns))
        Transfer.insert_df('team_geog', df_add)
