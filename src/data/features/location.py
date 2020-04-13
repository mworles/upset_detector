from src.data import Transfer
from src.data import Clean
from src.data import Generate
import pandas as pd
import numpy as np
from geopy.distance import great_circle
import re
from datetime import datetime
import unicodedata


def clean_schedule_date(x):
    datestr = ' '.join([r.strip() for r in x.split(',')[1:]])
    date = datetime.strptime(datestr, "%b %d %Y")
    date = date.strftime("%Y/%m/%d")
    return date

def remove_unicode(x):
    try:
        x = unicodedata.normalize("NFKD", x)
        x = x.encode('utf-8').strip()
        x = x.encode('ascii', 'ignore')
    except:
        print x
        pass
    return x

def game_info_home(mod=None):
    # pull game info for all seasons > 2002
    # need game_id and numeric ids of teams in each game
    # have scraped gym locations starting with 2003
    gi = Transfer.return_data('game_info', modifier=mod)
    gi = gi.drop(['t1_score', 't2_score', 't1_win', 't1_marg'], axis=1)

    # pull team_home data for same seasons
    th = Transfer.return_data('team_home', modifier=mod)
    # keep home team rows bc need to identify home team for game locations
    th = th[th['home'] == 1]
    th = th.drop(['date', 'home'], axis=1)
    th = th.rename(columns={'team_id': 'home_id'})
    merge_on = ['game_id']

    # combine game info and team home data
    df = pd.merge(gi, th, left_on='game_id', right_on='game_id', how='left')
    
    # add column indicating location is at neutral site or not
    df['neutral'] = np.where(df['home_id'].isnull(), 1, 0)
    
    return df


def city_locations(full_state=True):
    usc = Transfer.return_data('us_cities')
    uss = Transfer.return_data('us_states')
    uss = uss.rename(columns={'ID': 'ID_STATE'})
    ct = pd.merge(usc, uss, left_on='ID_STATE', right_on='ID_STATE',how='inner')


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
    
    # add values for foreign countries
    map_update = {'VI': 'Virgin Islands',
                  'BA': 'Bahamas',
                  'MX': 'Mexico',
                  'CI': 'Cayman Islands',
                  'JA': 'Jamaica',
                  'IR': 'Ireland'}
    state_map.update(map_update)
    # use dict to assign state_name values
    sc['STATE_NAME'] = sc['STATE_CODE'].map(state_map)
    ct = pd.concat([ct, sc], sort=False)
    
    if full_state == True:
        state = 'STATE_NAME'
    else:
        state = 'STATE_CODE'
    
    ct = ct[['CITY', state, 'LATITUDE', 'LONGITUDE']]
    ct = ct.drop_duplicates(subset=['CITY', state])
    ct['CITY'] = ct['CITY'].str.replace('Saint', 'St.')
    
    ct['city_state'] = zip(ct['CITY'].values, ct[state].values)
    ct = ct.set_index('city_state')
    cd = ct[['LATITUDE', 'LONGITUDE']].to_dict('index')
    return cd


def get_city_coordinates(city_state, cities):
    try:
        city_dict = cities[city_state]
        result = (city_dict['LATITUDE'], city_dict['LONGITUDE'])
    except:
        result = None
    
    return result

def get_team_coordinates(team_id, team_cities, cities):
    team_dict = team_cities[team_id]
    city_state = (team_dict['city'], team_dict['state'])
    lat_lng = get_city_coordinates(city_state, cities)
    td = {'team_id': team_id, 'loc': (lat_lng[0], lat_lng[1])}
    return td

def new_team_geog(team_cities):
    cities = city_locations()
    
    new_geog = lambda x: get_team_coordinates(x, team_cities, cities)
    new_teams = map(new_geog, team_cities.keys())
    df = pd.DataFrame(new_teams)
    return df
    

def team_geography():
    tg = Transfer.return_data('team_geog')
    tg['loc'] = zip(tg['lat'].values, tg['lng'].values)

    team_cities = {1465: {'city': 'Riverside', 'state': 'California'},
                   1466: {'city': 'Florence', 'state': 'Alabama'}}
    tg_add = new_team_geog(team_cities)
    tg = pd.concat([tg, tg_add], sort=False)
    tg = tg.set_index('team_id')
    tgd = tg[['loc']].to_dict('index')
    return tgd

def make_game_id(df, id_cols):
    seas = Transfer.return_data('seasons')
    seas = seas.loc[:, ['season', 'dayzero']]
    df = pd.merge(df, seas, how='inner', left_on='season', right_on='season')
    df['date'] = df.apply(Clean.game_date, axis=1)
    df = df.drop(['dayzero'], axis=1)
    df = Generate.convert_team_id(df, id_cols, drop=True)
    df = Generate.set_gameid_index(df, date_col='date', full_date=True, 
                                   drop_date=True)
    df = df.sort_index()
    df = df.reset_index()
    return df


def tourney_geog(mod=None):
    mod = "WHERE season < 2010"
    tg = Transfer.return_data('tourney_geog', modifier=mod)

    # add season data containing season starting date as 'dayzero'
    tg = make_game_id(tg, ['wteam', 'lteam'])
    tg['game_loc'] = zip(tg['lat'].values, tg['lng'].values)
    tg = tg[['game_id', 'game_loc']]
    return tg

def team_key_schedule():
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
    tk = team_key_schedule()
    tkm = tk[['team_id', 'team_sched']].rename(columns={'team_sched': 'team'}).copy()
    df = pd.merge(df, tkm, left_on='team', right_on='team', how='inner')

    # merge numeric ids for opponents
    tkm = tk[['team_id', 'team_ss']].rename(columns={'team_ss': 'opponent',
                                                     'team_id': 'opp_id'}).copy()
    df = pd.merge(df, tkm, left_on='opponent', right_on='opponent', how='inner')

    keep_cols = ['season', 'date', 'location', 'gym', 'team_id', 'opp_id']
    df = df[keep_cols]
    
    return df

def clean_gym(x):
    gym = x.lower()
    gym = re.sub(r"\(.*\)", "", gym).rstrip()
    return gym

# import gym location data
def gym_cities(modifier=None):
    df = Transfer.return_data('game_gym', modifier=modifier)
    df = df[['gym', 'city', 'state']].drop_duplicates()
    df['city'] = df['city'].str.replace('-', ' ')
    df['city'] = df['city'].str.replace('Saint', 'St.')
    df['gym'] = df['gym'].str.lower()
    df = df.apply(lambda x: x.str.strip())
    df = df[~df['gym'].duplicated()]
    df = df.set_index('gym')
    gym_dict = df.to_dict('index')
    gym_dict['agganis arena'] = {'city': 'Boston', 'state': 'Massachusetts'}
    return gym_dict

def get_gym_location(x, gym_dict):
    try:
        #location = (gym_dict[x]['city'].strip(), gym_dict[x]['state'].strip())
        location = (gym_dict[x]['city'], gym_dict[x]['state'])
        return location
    except:
        location = None
    return location

def schedule_gyms(modifier=None):
    # import schedule data that has gym names
    df = Transfer.return_data('cbb_schedule', modifier=modifier)
    # only need neutral site games
    df = df[df['location'] == 'N']
    # only use games with gym values
    df = df[df['gym'].notnull()]
    
    # clean gym values for bettter alignment with gym city data
    df['gym'] = map(clean_gym, df['gym'].values)

    # convert date to the project's standard date format
    df['date'] = map(clean_schedule_date, df.date.values)

    # clean up opponent names by removing team ranks
    remove_rank = lambda x: re.sub(r"\(\d*\)", "", x).rstrip()
    df['opponent'] = map(remove_rank, df['opponent'].values)
    
    # obtain team numeric ids and game id for each game
    df = ids_to_schedule(df)
    df = Generate.convert_team_id(df, ['team_id', 'opp_id'], drop=True)
    
    df = Generate.set_gameid_index(df, date_col='date', full_date=True, 
                                   drop_date=True)
    df = df.sort_index()
    
    # isolate unique gym names for matching to gym locations
    df = df[['gym']]
    
    return df

def gym_locations(modifier=None):
    # obtain df of unique gym names from 2003-2010 schedule
    gid_gyms = schedule_gyms(modifier=modifier)
    game_gyms = gid_gyms.drop_duplicates()

    # dict with keys as gym names and values as (city_state) locations
    gym_dict = gym_cities()

    # isolate gyms with and without a match in gym_dict
    in_dict = game_gyms['gym'].isin(gym_dict.keys())
    gyms_in = game_gyms[in_dict].copy()
    gyms_out = game_gyms[~in_dict].copy()

    # lambda function to return location for a given gym name
    find_location = (lambda x: get_gym_location(x, gym_dict))
    # add city_state to gym data
    gyms_in['city_state'] = map(find_location, gyms_in['gym'].values)

    # now use fuzzy string matching to search for additional gym locations
    # gyn names without a perfect match in gym_dict
    gyms_to_find = gyms_out['gym'].values

    # list of all keys in gym_dict to search for fuzzy match
    gym_loc_names = gym_dict.keys()

    # lambda function for fuzzy matching gym names
    match_gyms = lambda x: Clean.fuzzy_match(x, gym_loc_names, cutoff=90)

    # use lambda func to add fuzzy matches to schedule gyms data
    gyms_out['gym_fuzz'] = map(match_gyms, gyms_to_find)

    # get location for each fuzzy match
    # gyms without a fuzzy match will have null values
    gyms_out['city_state'] =  map(find_location, gyms_out['gym_fuzz'].values)
    gyms_out = gyms_out.drop(['gym_fuzz'], axis=1)

    # combine merged and fuzzy matched gym location data
    gyms = pd.concat([gyms_in, gyms_out], sort=False)
    

    # dict of coordinates for cities
    cities = city_locations(full_state=True)
    city_location = lambda x: get_city_coordinates(x, cities)
    
    gyms['game_loc'] = map(city_location, gyms['city_state'].values)

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
    
    return gym_dict


def gym_coordinates(x, gym_dict):
    try:
        gd = gym_dict[x]
        result = (gd['lat'], gd['lng'])
    except:
        if 'agrelot' in x:
            gd = gym_dict['josa miguel agrelot coliseum']
            result = (gd['lat'], gd['lng'])
        else:
            result = None
    
    return result

def game_gym_coordinates(x, gym_dict):
    try:
        result = gym_dict[x]['game_loc']
    except:
        result = None
    
    return result


df = game_info_home()

# select non-neutral games to get location of home team
dfh = df[df['neutral'] == 0].copy()
team_geog = team_geography()

home_location = lambda x: team_geog[x]['loc']
dfh['game_loc'] = map(home_location, dfh['home_id'].values)
dfh = dfh.set_index('game_id')


# identify all neutral games
dfn = df[df['neutral'] == 1].copy()

# import location of all tourney games 1985-2009
mod = "WHERE season < 2010"
tg = tourney_geog(mod)

# right merge to neutral games
# merge provides game_loc column 
dfloc1 = pd.merge(dfn, tg, how='right', left_on='game_id', right_on='game_id')

# remove 1985-2009 tourney games from neutral set
dfn = dfn[~dfn['game_id'].isin(dfloc1['game_id'].values)]

# set index as game id
dfloc1 = dfloc1.set_index('game_id')


# import game cities data with all seasons >= 2010
gc = Transfer.return_data('game_cities')
# merge in city and state names
ct = Transfer.return_data('cities')

gc = pd.merge(gc, ct, how='inner', left_on='city_id', right_on='city_id')
gc['city_state'] = zip(gc['city'].values, gc['state'].values)

gc = make_game_id(gc, ['wteam', 'lteam'])
gc = gc.loc[:, ['game_id', 'city_state']]

# select remaining neutral games >= 2010
dfloc2 = dfn[dfn['season'] >= 2010].copy()

# right merge game cities
dfloc2 = pd.merge(dfloc2, gc, how='left', left_on='game_id', right_on='game_id')

cities = city_locations(full_state=False)
city_location = lambda x: get_city_coordinates(x, cities)
dfloc2['game_loc'] = map(city_location, dfloc2['city_state'].values)

# keep games with locations found
dfloc2 = dfloc2[dfloc2['game_loc'].notnull()]
dfloc2 = dfloc2.set_index('game_id')
dfloc2 = dfloc2.drop(['city_state'], axis=1)

# remove neutral games with season >= 2010 that have locations found
dfn = dfn[~dfn['game_id'].isin(dfloc2.index.values)]


# 3rd batch is games from 2003 - 2010, which have scraped gym locations
bool = (dfn['season'] >= 2003) & (dfn['season'] <= 2009)
dfloc3 = dfn[bool]

# obtain df games with gym names from 2003-2010 schedule
gid_gyms = schedule_gyms(modifier="WHERE season >= 2003")
gid_gyms = gid_gyms.reset_index()
gid_gyms = gid_gyms.drop_duplicates()

# merge gyms with games
dfloc3 = pd.merge(dfloc3, gid_gyms, how='left', left_on='game_id',
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


#df.to_pickle('df.pkl')
df = pd.read_pickle('df.pkl')

df['latitude'] = df['latitude'].round(3)
df['longitude'] = df['longitude'].round(3)

Transfer.insert_df('game_location', df, at_once=False)


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
