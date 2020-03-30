import pandas as pd
import time
from src.data import Transfer
from src.data import Clean

def team_from_href(href, strip=1):
    hs = href.split('/')[-2:]
    team, id = hs[1], hs[0]
    team = '-'.join(team.split('-')[:-strip])
    td = [{str(team): int(id)}]
    return td
    
def espn_team_info(team_row, strip=1):
    try:
        href = team_row.find_all('span')[-1].find('a')['href']
        td = team_from_href(href, strip)
    except:
        td = None
    return td

def espn_standings(season, strip=1):
    url = 'https://www.espn.com/mens-college-basketball/standings/_/season/'
    url += str(season)
    r = requests.get(url)
    soup = BeautifulSoup(r.content, 'html.parser')
    div_class = 'standings__table InnerLayout__child--dividers'
    divs = soup.find_all('div', {'class': div_class})
    tables = [d.find('table').find('tbody') for d in divs]
    table_rows = [t.findAll('tr') for t in tables]
    all_rows = [row for table in table_rows for row in table]
    teams = [espn_team_info(x, strip) for x in all_rows]
    teams = [x for x in teams if x is not None]
    teams = [dict for team in teams for dict in team]
    teams = {k: v for d in teams for k, v in d.items()}
    return teams

def espn_team_season(team_id, season):
    bl = 'https://www.espn.com/mens-college-basketball/team/stats/_/id'
    espn_url = '/'.join([bl, str(team_id), 'season', str(season)])
    return espn_url

def espn_player_table(team_id, season):
    url = espn_team_season(team_id, season)
    r = requests.get(url)
    time.sleep(1)
    soup = BeautifulSoup(r.content, 'html.parser')
    try:
        div = soup.find(text = 'Per Game Stats').parent.findNext('div')
        t_names = div.find('table')
        name_rows = t_names.findAll('tr')[1:-1]
        names = [r.find('a').getText() for r in name_rows]
        names = [str(remove_unicode(x)).lower() for x in names]
        t_stats = div.find('div').find('table')
        col_names = [x.getText() for x in t_stats.find('thead').find_all('th')]
        col_names = [str(x.replace('%', '')).lower() for x in col_names]
        col_names = ['tov' if x=='to' else x for x in col_names]
        stat_rows = t_stats.find('tbody').findAll('tr')[:-1]
        stats = [[float(x.getText()) for x in r.find_all('td')] for r in stat_rows]
        data_rows = [[name] + row for name, row in zip(names, stats)]
        col_names.insert(0, 'name')
    except:
        col_names = []
        data_rows = [[]]

    col_names.insert(0, 'season')
    data_rows = [[season] + x for x in data_rows]

    data_rows.insert(0, col_names)
    return data_rows

def alternate_names():
    """Obtain dataframe with team id, team name, and alternate name versions."""
    # import team key data from table
    tk = Transfer.return_data('team_key')
    # clean and use team_ss as team name to match team in player_pergame table
    tk = tk[tk['team_ss'] != 'NULL']
    tk['team'] = map(Clean.school_name, tk['team_ss'].values)
    tk = tk[['team_id', 'team']].drop_duplicates()

    # import, merge, and modify alternate names to align with espn team names
    ts = Transfer.return_data('team_spellings')
    tk = pd.merge(tk, ts, how='left', left_on='team_id', right_on='team_id')
    tk['name_spelling'] = tk['name_spelling'].str.replace(' ', '-')
    tk['name_spelling'] = tk['name_spelling'].str.replace('&', '')
    tk = tk.drop_duplicates()
    return tk

def teams_missing_minutes(season):
    """Return dataframe containing teams to obtain player data."""
    # get teams from player_pergame table with any missing minutes
    # select rows using the season argument
    mod = "WHERE min_pg IS NULL AND season = %s" % (season)
    df = Transfer.return_data('player_pergame', modifier=mod)
    df = df[['team', 'season']].drop_duplicates()
    
    # merge with modified team key to use alternate team name options
    tk = alternate_names()
    df = pd.merge(df, tk, how='left', left_on='team', right_on='team')
    # no team name options to use if not merged, so drop these rows
    df = df[df['team_id'].notnull()]
    
    # find teams with data already obtained in espn_pergame table
    mod = "WHERE season = %s" % (season)
    epg = Transfer.return_data('espn_pergame', modifier=mod)
    
    # create list of teams obtained
    try:
        epg_teams = pd.unique(epg['team'])
    # above will error if no teams, so create empty list
    except:
        epg_teams = []
    
    # subset reflects teams needing minutes that aren't in espn_pergame
    df = df[~df['team'].isin(epg_teams)]
    df = df['team_id'].sort_values()
    
    return df

def get_espn_id(name_versions, espn_ids):
    """Use alternate names to identify numeric id for team on espn site."""
    # get overlap between name versions and espn names stripped by 1
    both = set(name_versions).intersection(set(espn_ids[0].keys()))
    
    # if overlap is one team, obtain numeric id
    if len(both) == 1:
        matching_name = list(overlap)[0]
        espn_id = espn_ids[0][matching_name]
    else:
        # get overlap between name versions and espn2 names stripped by 2
        both = set(name_versions).intersection(set(espn_ids[1].keys()))
        
        # if overlap is one team, scrape and return player stats table
        if len(both) == 1:
            matching_name = list(overlap)[0]
            espn_id = espn_ids[1][matching_name]
        # if overlap is 0 or 2 or more, return None
        else:
            espn_id = None
    
    return espn_id

def espn_from_season(season):
    """Obtain data needed from espn site for season."""
    
    # df of teams needing data
    df_needed = teams_missing_minutes(season)
    
    # obtain dicts of team names and numeric ids from espn standings site
    # raw team names on site include team and mascot
    # get versions of team names with final 1 and 2 elements removed
    espn_ids = [espn_standings(season, strip=1)]
    espn_ids.append(espn_standings(season, strip=2))
    
    # loop over unique teams neeeding data
    for team_id in list(set(df_needed['team_id'])):
        # boolean index for team's rows
        t_rows = df['team_id'] == team_id
        # identify all name versions and the player_pergame team name
        name_versions = df.loc[t_rows, 'name_spelling'].values
        
        espn_id = get_espn_id(name_versions, espn_ids)
        
        # create lists without data if no id found
        if espn_id is None:
            data = [['season'], [season]]
        # scrape lists of data for given espn team id and season
        else:
            data =espn_player_table(espn_id, season)
        
        # obtain team name used in player_pergame table
        team = pd.unique(df.loc[t_rows, 'team'])[0]
        data[0] = ['team'] + data[0]
        data[1:] = [[team] + x for x in data[1:]]
        
        # insert results to msyql table
        Transfer.insert('espn_pergame', data, at_once=True)    
