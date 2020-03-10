from bs4 import BeautifulSoup
import requests
import datetime
import requests
import unicodedata
import time
import json
import re
import pandas as pd
import Clean

def team_ratings(url = 'http://kenpom.com/index.php'):
    """
    Reads team ratings data from kenpom website.
    """
    f = requests.get(url)
    soup = BeautifulSoup(f.text, features='lxml')
    t = soup.find('table', {'id': 'ratings-table'})
    thead = t.find('thead')
    thr_list = thead.find_all('tr')[-1].find_all('th')
    names = [x.getText() for x in thr_list]
    
    # add suffix to repeated column names
    repeats = {'AdjEM' : 0}
    prefix_gens = {'AdjEM': ['', 'Opp', 'OppNC']}

    for idx,s in enumerate(names):
            try:
                prefix = prefix_gens[s][repeats[s]]
            except KeyError:
                continue
            else:
                names[idx] = prefix + s
                repeats[s] +=1
    
    tr = t.tbody.find_all('tr')
    
    tds = [x.find_all('td') for x in tr]
    tds = [x for x in tds if len(x) > 0]
    
    def no_span(x):
        has_span = td.find('span')
        if has_span is None:
            return x.getText()
        else:
            return None

    def clean_text(x):
        x = x.replace('+', '')
        return x

    rows = [[no_span(td) for td in row] for row in tds]
    rows = [[x for x in row if x is not None] for row in rows]
    rows = [[clean_text(x) for x in row] for row in rows]
    
    date = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    [r.insert(0, date) for r in rows]
    
    names.insert(0, 'date')
    
    rows.insert(0, names)

    return rows


def get_table_rows(url):
    r = requests.get(url)
    soup = BeautifulSoup(r.content, 'html.parser')
    table = soup.find('table', {'class': 'frodds-data-tbl'})
    table_rows = table.findAll('tr')
    game_rows = [x for x in table_rows if len(x.findAll('td')) > 1]
    return game_rows

def encode_odds_cell(x):
    x = unicodedata.normalize("NFKD", x)
    x = x.encode('ascii', 'ignore').strip()
    x = x.replace('+', '')
    return x

def parse_odds_row(row):
    td = row.find('td')
    date_time = td.find('span').getText()
    date = date_time.split(' ')[0]
    time = " ".join(date_time.split(' ')[2:])
    teams = [a.getText() for a in td.findAll('a')]
    tdo = row.findAll('td')[2]
    tdbr = tdo.findAll('br')
    ml = [encode_odds_cell(x.next_sibling) for x in tdbr]
    row_data = [date, time] + teams + ml
    return row_data

def scrape_odds(url="https://www.vegasinsider.com/college-basketball/odds/las-vegas/money/"):
    game_rows = get_table_rows(url)
    data = [parse_odds_row(row) for row in game_rows]
    date = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    [r.insert(0, date) for r in data]
    columns = ['timestamp', 'game_date', 'game_time', 'team_1', 'team_2', 'odds1', 'odds2']
    data.insert(0, columns)
    return data

def parse_row_oddsportal(x):
    row_classes = x['class']
    if 'nob-border' in row_classes:
        row_text = x.find('th').get_text()
        global dt
        dt = row_text.split(' ')[0:3]
        dt.insert(0, dt.pop(1))
        dt = "/".join(dt)
        result = "NA"
    elif "deactivate" in row_classes:
        row_cells = x.find_all('td')
        row_text = [c.get_text() for c in row_cells]
        team_text = row_text[1].split(' - ')
        row_data = [dt] + team_text + [row_text[3], row_text[4]]
        result = row_data
    else:
        result = "NA"
    
    return result

def store_data_oddsportal(datdir, data, year, page):
    year_file = "".join([datdir, "odds", "_", str(year), ".json"])
    
    # open results logging json file if it exists
    try:
        with open(year_file) as f:
            year_data = json.load(f)
    except:
        year_data = {}
    
    # update scores dict with results of current run
    year_data[str(page)] = data

    # save results file as json
    with open(year_file, 'w') as f:
        json.dump(year_data, f, indent=4)


def make_url_oddsportal(year):
    urlb = 'https://www.oddsportal.com/basketball/usa/ncaa'
    suf = '/results/'
    year_string = "".join(['-', str(year - 1), '-', str(year)])
    if year < 2020:
        url = urlb + year_string + suf
    else:
        url = urlb + suf
    return url

def get_table_oddportal(soup):
    t = soup.find('table', {'class': ' table-main'})
    rows = [x for x in t.find_all('tr')]
    data = map(lambda x: parse_row_oddsportal(x), rows)
    data = [x for x in data if x is not "NA"]
    return data

def scrape_oddsportal(datdir, year):
    all_data = []
    browser = webdriver.Chrome('driver/chromedriver.exe')
    url = make_url_oddsportal(year)
    browser.get(url)
    
    time.sleep(4)
    
    soup = BeautifulSoup(browser.page_source, 'html.parser')

    pagin = soup.find('div', {'id': 'pagination'}).find_all('a')
    lh = pagin[-1]['href']
    lp = int(re.findall(r'\d+', lh)[0])
    
    page = 1
    data = get_table_oddportal(soup)
    store_data_oddsportal(datdir, data, year, page)
    all_data.extend(data)
    need_pages = range(2, lp + 1)
    
    for n in need_pages:
        try:
            nav = browser.find_element_by_link_text(str(n))
            nav.click()
            time.sleep(4)
            soup = BeautifulSoup(browser.page_source, 'html.parser')
            data = get_table_oddsportal(soup)
            store_data_oddsportal(data, year, n)
            all_data.extend(data)
        except:
            pass

    browser.quit()
    return all_data

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
        r = requests.get(url)
        gid = re.findall(r'\d+', url.split('/')[-1])[0]
        soup = BeautifulSoup(r.content, 'html.parser')
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
        print e
        return [['gid'], [gid]]


def game_scores(date, future=False):
    time.sleep(1)
    timestamp = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    url = tcpalm_url(date)
    r = requests.get(url)
    soup = BeautifulSoup(r.content, 'html.parser')

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
        print e
        games = [['date', 'timestamp'], [date, timestamp]]

    return games

def game_schedule(date):
    time.sleep(1)
    timestamp = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    url = tcpalm_url(date)
    r = requests.get(url)
    soup = BeautifulSoup(r.content, 'html.parser')

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
    r = requests.get(url)
    soup = BeautifulSoup(r.content, 'html.parser')
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

def get_boxes(date):
    timestamp = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    links = box_links(date)
    boxes = []
    
    for link in links:
        # small delay for repeated website requests
        time.sleep(1)
        boxes.append(game_box(link))
    
    [b[0].extend(['date', 'timestamp']) for b in boxes]
    [b[1].extend([date, timestamp]) for b in boxes]
    
    return boxes


def season_table(season):
    data_out = '../data/external/sportsref_schoolstats/'
    baseurl = "http://www.sports-reference.com/cbb/seasons/"
    url = baseurl + str(season) + '-school-stats.html'
    r = requests.get(url)
    soup = BeautifulSoup(r.content, 'html.parser')
    table = soup.find("table")
    return table

def remove_unicode(x):
    try:
        x = unicodedata.normalize("NFKD", x)
        x = x.encode('utf-8').strip()
        x = x.encode('ascii', 'ignore')
    except:
        pass
    return x

def parse_pergame(soup):
    col_map = {'Player': 'name',
               'G': 'g',
               'GS': 'g_start',
               'MP': 'min_pg',
               'FG': 'fgm_pg',
               'FGA': 'fga_pg',
               'FG%': 'fg_pct',
               '2P': 'fg2m_pg',
               '2PA': 'fg2a_pg',
               '2P%': 'fg2_pct',
               '3P': 'fg3m_pg',
               '3PA': 'fg3a_pg',
               '3P%': 'fg3_pct',
               'FT': 'ftm_pg',
               'FTA': 'fta_pg',
               'FT%': 'ft_pct',
               'ORB': 'rbo_pg',
               'DRB': 'rbd_pg',
               'TRB': 'rb_pg',
               'AST': 'ast_pg',
               'STL': 'stl_pg',
               'BLK': 'blk_pg',
               'TOV': 'to_pg',
               'PF': 'pf_pg',
               'PTS': 'pts_pg'}

    table = soup.find('table', {'id': 'per_game'})
    rows = table.findAll('tr')
    cols = [th.getText() for th in rows[0].findAll('th')][1:]
    col_names = [col_map[c] for c in cols]

    data = [[td.getText() for td in row.findAll('td')] for row in rows[1:]]
    data = [[remove_unicode(x) for x in row] for row in data]
    data.insert(0, col_names)
    return data

def parse_rosters(soup):
    col_map = {'Player': 'name',
               '#': 'number',
               'Class': 'class',
               'Pos': 'position',
               'Height': 'height',
               'Weight': 'weight',
               'Hometown': 'home_town',
               'High School': 'high_school',
               'RSCI Top 100': 'rsci',
               'Summary': 'summary'}
    
    try:
        table = soup.find("table", {'id': 'roster'})
        rows = table.findAll('tr')
    except:
        print len(soup.find_all('table'))
    
    cols = [th.getText() for th in rows[0].findAll('th')]
    col_names = [col_map[c] for c in cols]
    len_data = len(rows[1:])
    
    names = [row.find('th').getText() for row in rows[1:]]
    names = [remove_unicode(x) for x in names]
    data = [[td.getText() for td in row.findAll('td')] for row in rows[1:]]
    data = [[remove_unicode(x) for x in row] for row in data]
    result = [[n] + d for n, d in zip(names, data)]
    result.insert(0, col_names)
    return result


def add_season_team(table, season, team):
    table_cols = ['season', 'team'] + table[0]
    data = table[1:]
    new_table = [[season, team] + d for d in data]
    new_table.insert(0, table_cols)
    return new_table

def team_page(url):
    time.sleep(2)
    r = requests.get(url)
    soup = BeautifulSoup(r.content, 'html.parser')
    return soup

def srcbb_team(season):
    table = season_table(season)
    rows = table.findAll('tr')
    base = 'https://www.sports-reference.com'
    
    links = []
    for row in rows:
        try:
            a = row.find('a')
            href = a['href']
            link = "".join([base, href])
            links.append(link)
        except:
            pass
    
    for url in links:
        url_split = url.split('/')
        season = int(url_split[-1].split('.')[0])
        team = remove_unicode(url_split[-2])
        soup = team_page(url)

        try:
            roster = parse_rosters(soup)
            roster = add_season_team(roster, season, team)
            Transfer.insert('team_roster', roster, at_once=True)
        except:
            print url
            fail = [['season', 'team'], [season, team]]
            Transfer.insert('team_roster_error', fail, at_once=True)
            
        try:
            per_game = parse_pergame(soup)
            per_game = add_season_team(per_game, season, team)
            Transfer.insert('player_pergame', per_game, at_once=True)
        except:
            print url
            fail = [['season', 'team'], [season, team]]
            Transfer.insert('player_pergame_error', fail, at_once=True)

def game_gym(url):
    gid = re.findall(r'\d+', url.split('/')[-1])[0]
    try:
        r = requests.get(url)
        soup = BeautifulSoup(r.content, 'html.parser')
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

def tcpalm_(date):
    timestamp = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    links = box_links(date)
    for l in links:
        print l
    gyms = []
    
    for link in links:
        # small delay for repeated website requests
        time.sleep(1)
        gyms.append(game_gym(link))
    
    season = Clean.season_from_date(date)
    [row[0].extend(['season', 'date', 'timestamp']) for row in gyms]
    [row[1].extend([season, date, timestamp]) for row in gyms]
    
    return gyms

def parse_schedule(soup):
    col_map = {'Player': 'name',
               '#': 'number',
               'Class': 'class',
               'Pos': 'position',
               'Height': 'height',
               'Weight': 'weight',
               'Hometown': 'home_town',
               'High School': 'high_school',
               'RSCI Top 100': 'rsci',
               'Summary': 'summary'}
    
    loc_map = {'': 'H', '@': 'A', 'N': 'N'}
    
    table = soup.find("table", {'id': 'schedule'})
    rows = table.findAll('tr')

    cols = [th.getText() for th in rows[0].findAll('th')]
    #col_names = [col_map[c] for c in cols]
    date_i = cols.index('Date') - 1
    opp_i = cols.index('Opponent') - 1
    gym_i = cols.index('Arena') - 1
    
    data = [[td.getText() for td in row.findAll('td')] for row in rows[1:]]
    data = [row for row in data if len(row) > 0]
    dates = [x[date_i] for x in data]
    opp = [x[opp_i] for x in data]
    gym = [x[gym_i] for x in data]
    loc = [x[2] for x in data]
    loc = [loc_map[x] for x in loc]
    cols = [dates, loc, opp, gym]
    tab = map(list, zip(*cols))
    tab = [[remove_unicode(x) for x in row] for row in tab]
    tab.insert(0, ['date', 'location', 'opponent', 'gym'])
    return tab


def srcbb_schedule_links(season):
    table = season_table(season)
    rows = table.findAll('tr')
    base = 'https://www.sports-reference.com/cbb/schools/'

    links = []
    
    base = 'https://www.sports-reference.com'
    
    scheds = []
    
    links = []
    for row in rows:
        try:
            a = row.find('a')
            href = a['href']
            link = "".join([base, href])
            link = link.replace('.html', '-schedule.html')
            links.append(link)
        except:
            pass
    
    return links

def srcbb_schedule(url):
    soup = team_page(url)
    sched = parse_schedule(soup)
    return sched

def team_from_url(url):
    url_split = url.split('/')
    team = remove_unicode(url_split[-2])
    return team
