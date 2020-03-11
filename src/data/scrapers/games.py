import utils
import time
import re

def decode(x): 
    return str(x.encode('utf-8').strip().decode('ascii', 'ignore'))

def tcpalm_score(x):
    score = decode(x)
    try:
        int(score)
        return score
    except:
        return ''

def tcpalm_game(table):
    trs = table.findAll('tr')[1:]
    tds = [r.findAll('td')[-1] for r in trs]
    scores = [td.getText() for td in tds]
    scores = [tcpalm_score(x) for x in scores]
    return scores

def tcpalm_team(team):
    name = re.sub(r'\([0-9]*\)', '', team)
    return decode(name)

def tcpalm_header(header):
    text = header.getText()
    
    if ' at ' in text:
        neutral = 0
        split_on = ' at '
    elif ' vs ' in text:
        neutral = 1
        split_on =  'vs '
    
    teams = text.split(split_on)
    names = [tcpalm_team(x) for x in teams]
    
    team_info = names + [neutral]
    return team_info

def tcpalm_section(div, future=False):
    headers = div.findAll('div', {'class': 'sdi-so-title'})
    gids = [decode(h['id'].replace('Title_7_', '')) for h in headers]
    tables = div.findAll('table')
    team_info = [tcpalm_header(x) for x in headers]
    if future == False:
        scores = [tcpalm_game(x) for x in tables]
        game_info = [[i] + t + s for i, t, s, in zip(gids, team_info, scores)]
    else:
        game_info = [[i] + t for i, t, in zip(gids, team_info)]
    return game_info

def tcpalm_url(date):
    urlb = 'http://stats.tcpalm.com/sports-scores/College-Basketball-Scores-Matchups.aspx?'
    date_split = date.split('/')
    year_i = int(date_split[0])
    year = "Year=" + date_split[0]
    period = "Period=" + "".join(date_split[1:])
    month = int(date_split[1])
    if month > 9:
        season = "".join(["CurrentSeason=", str(year_i), "-", str(year_i+1)])
    else:
        season = "".join(["CurrentSeason=", str(year_i-1), "-", str(year_i)])
    url_suf = "&".join([year, period, season])
    url = "".join([urlb, url_suf])
    return url

def box_row(row):
    row_cells = [td.getText() for td in row.findAll('td')]
    row_text = [decode(x) for x in row_cells]
    return row_text

def process_team(div):
    box_rows = div.findAll('tr')
    box_data = [box_row(row) for row in box_rows]
    row_leads = [x[0] for x in box_data]
    total_i = row_leads.index('TOTAL')
    team_totals = box_data[total_i][1:]
    team_cols = box_data[1][1:]
    team_cols = [c.replace('-', '') for c in team_cols]
    team_dict = {k:v for k, v in zip(team_cols, team_totals)}
    return team_dict

def game_box(url):
    gid = re.findall(r'\d+', url.split('/')[-1])[0]
    try:
        soup = utils.get_soup(url)
        gid = re.findall(r'\d+', url.split('/')[-1])[0]
        divs = soup.findAll('div', {'class': 'sdi-so'})
        div_teams = divs[0]
        divs_box = divs[1:3]
        #teams = div_teams.findAll('td', {'class': 'sdi-datacell'})
        td_names = [r.find('td') for r in div_teams.findAll('tr')[1:3]]
        team_names = [decode(t.getText()) for t in td_names]
        team_dicts = [process_team(div) for div in divs_box]
        ucols = list(set([k for td in team_dicts for k in td.keys()]))
        ucols.sort()
        team_data = [[td[c] if c in td.keys() else '' for c in ucols] for td in team_dicts]
        game_data = [val for team_row in team_data for val in team_row]
        # insert team name columns and data to start of lists
        game_data = team_names + game_data
        cols_both = ['away_' + c for c in ucols] + ['home_' + c for c in ucols]
        cols_all = ['away_team', 'home_team'] + cols_both
        # box id
        cols_all.insert(0, 'gid')
        game_data.insert(0, gid)
        return [cols_all, game_data]
    
    except Exception as e:
        return [['gid'], [gid]]

def game_scores(date, future=False):
    time.sleep(1)
    timestamp = utils.current_timestamp()
    url = tcpalm_url(date)
    soup = utils.get_soup(url)

    try:
        div = soup.find('div', {'class': 'sdi-divScoreColumn_1-2'})
        games = tcpalm_section(div, future=future)
        try:
            div2 = soup.find('div', {'class': 'sdi-divScoreColumn_2-2'})
            div2_games = tcpalm_section(div2, future=future)
            games.extend(div2_games)
        except:
            pass

        games = [g + [date] + [timestamp] for g in games]
        
        col_names = ['gid', 'away_team', 'home_team', 'neutral', 'away_score',
                     'home_score', 'date', 'timestamp']
        if future == True:
            col_names = [x for x in col_names if '_score' not in x]

        games.insert(0, col_names)
        
    except Exception as e:
        games = [['date', 'timestamp'], [date, timestamp]]

    return games
    
def game_schedule(date):
    time.sleep(1)
    timestamp = utils.current_timestamp()
    url = tcpalm_url(date)
    soup = utils.get_soup(url)

    try:
        div = soup.find('div', {'class': 'sdi-divScoreColumn_1-2'})
        games = tcpalm_section(div, future=True)
        try:
            div2 = soup.find('div', {'class': 'sdi-divScoreColumn_2-2'})
            div2_games = tcpalm_section(div2, future=True)
            games.extend(div2_games)
        except:
            pass

        games = [g + [date] + [timestamp] for g in games]
        
        col_names = ['gid', 'away_team', 'home_team', 'neutral', 'date', 'timestamp']
        
        games.insert(0, col_names)
        
    except Exception as e:
        print e
        games = [['date', 'timestamp'], [date, timestamp]]

    return games

def box_links(date):
    url = tcpalm_url(date)
    soup = utils.get_soup(url)
    divs = soup.findAll('div', {'class': 'onoff'})

    base = 'http://stats.tcpalm.com'
    links = []
    for d in divs:
        for a in d.findAll('a'):
            if a.getText() == 'Boxscore':
                href = a['href']
                link = "".join([base, href])
                links.append(link)
                
    return links

def box_scores(date):
    timestamp = utils.current_timestamp()
    links = box_links(date)
    boxes = []
    
    for link in links:
        # small delay for repeated website requests
        time.sleep(1)
        boxes.append(game_box(link))
    
    [b[0].extend(['date', 'timestamp']) for b in boxes]
    [b[1].extend([date, timestamp]) for b in boxes]
    
    return boxes

def game_gym(url):
    gid = re.findall(r'\d+', url.split('/')[-1])[0]
    try:
        soup = utils.get_soup(url)
        div = soup.find('div', {'class': 'sdi-quickhits'})
        strongs = div.find_all('strong')
        strong_text = [remove_unicode(x.text) for x in strongs]
        sibs = [remove_unicode(x.next_sibling) for x in strongs]
        for x in zip(strong_text, sibs):
            if 'Where' in x[0]:
                location = x[1]
                if '(' in location:
                    location = re.sub(r'\([^)]*\)', '', location)
                # remove any extraneous info in parentheses
                loc_list = location.split(',')
            elif 'Attendance' in x[0]:
                if any(i.isdigit() for i in x[1]):
                    attendance = int(filter(str.isdigit, x[1]))
                else:
                    attendance = None
        gym_vals = loc_list + [attendance]
        gym_vals.insert(0, gid)
        cols = ['gid', 'gym', 'city', 'state', 'attendance']
        # adjust columns if some values not filled
        if len(gym_vals) < 5:
            if len(loc_list) == 2:
                cols.remove('state')
        return [cols, gym_vals]
    except Exception as e:
        print e
        return [['gid'], [gid]]

def get_gyms(season):
    mod = 'WHERE season = %s' % (season)
    games = Transfer.return_data('game_info', mod)
    dates = list(set(games['date']))
    if season == 2010:
        dates = [x for x in dates if x > '2009/12/15']
    dates.sort()
    for date in dates:
        print date
        timestamp = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        links = Scrapers.box_links(date)
        if len(links) > 1:
            for url in links:
                # small delay for repeated website requests
                time.sleep(1)
                try:
                    gym = Scrapers.game_gym(url)
                    gym[0].extend(['season', 'date', 'timestamp'])
                    gym[1].extend([season, date, timestamp])
                    Transfer.insert('game_gym', gym, at_once=True)
                except:
                    print url
                    gid = re.findall(r'\d+', url.split('/')[-1])[0]
                    gym = [['gid', 'season', 'date'], [gid, season, date]]
                    Transfer.insert('game_gym_error', gym, at_once=True)
        else:
            gym = [['season', 'date'], [season, date]]
            Transfer.insert('game_gym_error', gym, at_once=True)

def get_scheduled(dates):
    scheduled = []

    for date in dates:
        results = game_scores(date, future=True)    
        if len(scheduled) == 0:
            scheduled.extend(results)
        else:
            scheduled.extend(results[1:])
    
    return scheduled
