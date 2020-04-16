""" location

A module for transforming data pertaining to the geographical location of
games, places, or teams.

"""
from src import Constants
from src.data import Transfer
from src.data import Clean
from src.data import Generate
import pandas as pd
import numpy as np
import re
from datetime import datetime
from geopy.distance import great_circle

def run():
    """
    # insert any new team cities to team location table
    update_team_locations(Constants.TEAM_CITY_UPDATE)    
    
    # import games data with host team and site details
    mod = None #"WHERE date > '2018/04/30'"
    df = games_with_sites(mod)
    
    # select non-neutral games to get location of home team
    dfh = df[df['neutral'] == 0].copy()
    team_map = team_locations()
    
    home_id = dfh['home_id'].values
    dfh['game_loc'] = map(lambda x: locate_item(x, team_map), home_id)
    
    # identify all neutral games
    dfn = df[df['neutral'] == 1].copy()

    # import location of all tourney games 1985-2009
    # contains game_id and game_loc column with location coordinates
    games = game_locations()
    
    # right merge to neutral games
    dfn_has= pd.merge(dfn, games, how='right', left_on='game_id',
                      right_on='game_id')
    
    # remove obtained games from neutral set
    dfn = dfn[~dfn['game_id'].isin(dfn_has['game_id'].values)]

    
    # 3rd batch is games from 2003 - 2010, which have scraped gym locations
    bool = (dfn['season'] >= 2003) & (dfn['season'] <= 2009)
    dfn = dfn[bool]
    """
    df = locations_from_gym()

    """
    # obtain df games with gym names from 2003-2010 schedule
    game_gyms = gyms_from_schedule(modifier="WHERE season >= 2003")
    
    # merge gyms with games
    dfloc3 = pd.merge(dfloc3, game_gyms, how='left', left_on='game_id',
                      right_on='game_id')
    
    
    gym_dict = gym_locations(modifier="WHERE season >= 2003")

    game_location = lambda x: game_gym_coordinates(x, gym_dict)
    dfloc3['game_loc'] = map(game_location, dfloc3['gym'].values)

    # keep games with locations found
    dfloc3 = dfloc3[dfloc3['game_loc'].notnull()]
    dfloc3 = dfloc3.set_index('game_id')
    dfloc3 = dfloc3.drop(['gym'], axis=1)

    # remove neutral games with season >= 2010 that have locations found
    dfn = dfn[~dfn['game_id'].isin(dfloc3.index.values)]

    loc_list = [dfh, dfloc1, dfloc2, dfloc3]
    df = pd.concat(loc_list, sort=False)

    df['latitude'] = map(lambda x: x[0], df['game_loc'].values)
    df['longitude'] = map(lambda x: x[1], df['game_loc'].values)

    df = df.loc[:, ['latitude', 'longitude']]
    df = df.reset_index()

    df['latitude'] = df['latitude'].round(3)
    df['longitude'] = df['longitude'].round(3)

    Transfer.insert_df('game_location', df, at_once=False)
    """
    
def update_team_locations(team_cities):
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
        df_add = pd.DataFrame(new_data, columns=list(df.columns))
        Transfer.insert_df('team_geog', df_add)


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


def games_with_sites(modifier=None):
    """Import 'game info' data with home team identifiers.

    Parameters
    ----------
    modifier : str, optional, default None
        MYSQL string to specify subsets of rows (e.g., 'WHERE year > 2000')

    Returns
    -------
    df : DataFrame
        Contains game info with home team and game site.

    """
    # need game_id and numeric ids of teams in each game
    # have scraped gym locations starting with 2003
    df = Transfer.return_data('game_info', modifier=modifier)
    df = df.drop(['t1_score', 't2_score', 't1_win', 't1_marg'], axis=1)

    # to add data on where game was hosted
    th = home_teams(modifier=modifier)
    df = pd.merge(df, th, left_on='game_id', right_on='game_id', how='left')

    # add column indicating host is neutral or not
    df['neutral'] = np.where(df['home_id'].isnull(), 1, 0)

    return df


def home_teams(modifier=None):
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
    df = Transfer.return_data('team_home', modifier=modifier)

    # identify home team for game locations
    df = df[df['home'] == 1]
    df = df.drop(['date', 'home'], axis=1)
    df = df.rename(columns={'team_id': 'home_id'})

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


def game_locations(modifier=None):
    # import game cities data
    games = Transfer.return_data('game_cities', modifier=modifier)
    # merge in city and state names
    cities = Transfer.return_data('cities')

    df = pd.merge(games, cities, how='inner', left_on='city_id',
                  right_on='city_id')
    df['city_state'] = zip(df['city'].values, df['state'].values)
    
    df = Generate.make_game_id(df, ['wteam', 'lteam'])
    
    # import city map with state abbreviations
    city_map = city_locations(full_state=False)
    
    city_state = df['city_state'].values
    df['game_loc'] = map(lambda x: locate_item(x, city_map), city_state)
    
    df = df[['game_id', 'game_loc']]

    # import and combine tourney games prior to 2010
    tg = Transfer.return_data('tourney_geog', modifier=modifier)
    
    # add unique game id from teams and date
    tg = Generate.make_game_id(tg, ['wteam', 'lteam'])
    tg['game_loc'] = zip(tg['latitude'].values, tg['longitude'].values)
    df = df[['game_id', 'game_loc']]
    
    df = pd.concat([df, tg], sort=False)
    
    return df


def locations_from_schedule(modifier=None):
    df = games_from_schedule(modifier=modifier)
    # only need neutral site games
    df = df[df['location'] == 'N']
    # only use games with gym values
    df = df[df['gym'].notnull()]
    unique_gyms = df[['gym']].drop_duplicates()
    gym_map = gym_locations(unique_gyms)
    gyms = df['gym'].values
    df['game_loc'] = map(lambda x: locate_item(x, gym_map), gyms)
    return df


def games_from_schedule(modifier=None):
    # import schedule data that has gym names
    df = Transfer.return_data('cbb_schedule', modifier=modifier)

    # clean gym values for bettter alignment with gym city data
    df['gym'] = map(clean_gym, df['gym'].values)
    # convert date to the project's standard date format
    df['date'] = map(convert_schedule_date, df.date.values)
    # clean up opponent names by removing team ranks
    df['opponent'] = map(lambda x: re.sub(r"\(\d*\)", "", x).rstrip(),
                         df['opponent'].values)

    # obtain team numeric ids and game id for each game
    df = ids_to_schedule(df)
    df = Generate.make_game_id(df, ['team_id', 'opp_id'], convert_date=False)
    
    # isolate unique gym names for matching to gym locations
    df = df[['game_id', 'gym']]
    df = df.drop_duplicates()

    return df


def gym_locations(df):
    # dict with keys as gym names and values as (city_state) locations
    gym_map = gym_coordinates()

    # isolate gyms with and without a match in gym_dict
    in_dict = df['gym'].isin(gym_map.keys())
    df_in = df[in_dict].copy()
    df_out = df[~in_dict].copy()

    # function for fuzzy matching gym names
    def match_gyms(name, options):
        try:
            result = Clean.fuzzy_match(name, options, cutoff=90)
        except TypeError:
            result = None
        return result

    # use match_gyms to add fuzzy-matched gym names
    options = gym_map.keys()    
    names = df_out['gym'].values
    df_out['fuzz'] = map(lambda x: match_gyms(x, options), names)
    
    # combine merged and fuzzy matched gym location data
    gyms = pd.concat([df_in, df_out], sort=False)
    locate = np.where(gyms['fuzz'].isnull(), gyms['gym'], gyms['fuzz'])
    df['gym_loc'] = map(lambda x: locate_item(x, gym_map), locate)
    df = df.set_index('gym')
    gym_dict = df['gym_loc'].to_dict()
    """
    gym_names = df_in['gym'].values
    
    # add city_state to gym data
    df_in['city_state'] = map(lambda x: locate_item(x, gym_map), gym_names)

    
    # separate gyms with coordinates found and not found
    found = gyms['game_loc'].notnull()
    gyms_found = gyms[found]
    gyms_tof = gyms[~found]
    gyms_tof = gyms_tof.drop(['game_loc'], axis=1)

    # get gym locations from manual gym data
    gym_man = Transfer.return_data('gym_manual', modifier=None)
    gym_man = gym_man.set_index('gym').to_dict('index')

    gym_location = lambda x: gym_coordinates(x, gym_man)
    gyms_tof['game_loc'] = map(gym_location, gyms_tof['gym'].values)

    # combine gym location sets
    gym_coord = pd.concat([gyms_found, gyms_tof], sort=False)
    gym_coord = gym_coord.loc[:, ['gym', 'game_loc']]
    gym_dict = gym_coord.set_index('gym').to_dict('index')
    
    """
    return gym_dict
    

def convert_schedule_date(raw_date):
    """Converts raw team schedule date to standard format for the project."""
    # remove irrelevant day of week from string
    datestr = ' '.join([x.strip() for x in raw_date.split(',')[1:]])
    dt = datetime.strptime(datestr, "%b %d %Y")
    date = dt.strftime("%Y/%m/%d")
    
    return date


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


def ids_to_schedule(df):
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


def clean_gym(x):
    gym = x.lower()
    gym = re.sub(r"\(.*\)", "", gym).rstrip()
    return gym


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
    
    gym_dict = df['city_state'].to_dict()
    gym_dict['agganis arena'] = (42.357603, -71.068432)

    return gym_dict


"""
dfh['t1_loc'] = map(game_loc, dfh['t1_team_id'].values)
dfh['t2_loc'] = map(game_loc, dfh['t2_team_id'].values)

def travel_distance(x):
    distance = int(great_circle(x[0], x[1]).miles)
    return distance

game_team = zip(dfh['game_loc'].values, dfh['t1_loc'].values)
dfh['t1_dist'] = map(travel_distance, game_team)
game_team = zip(dfh['game_loc'].values, dfh['t2_loc'].values)
dfh['t2_dist'] = map(travel_distance, game_team)

"""
