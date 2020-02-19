from bs4 import BeautifulSoup
import requests
import datetime
import requests
import unicodedata
import time
import json
import re
import pandas as pd
#from selenium import webdriver



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


def update_spread_dict(x, row_dict):
    regex = "\d{3}"
    try:
        line = re.findall(regex, x)[0]
        row_dict['line'] = line
    except:
        result_enc = x.encode('utf-8').strip()

        if 'PK' in result_enc:
            row_dict['spread'] = '0'
        else:
            result_dec = result_enc.decode('ascii', 'ignore')
            #result_split = result_dec.split('-')
            try:
                result = result_dec.split('-')[1]
                uni_5 = 'xc2\\xbd\\xc2\\xa0'
                if uni_5 in result_enc.encode('string_escape'):
                    result = result + '.5'
                else:
                    result = result + '.0'
                row_dict['spread'] = result
            except:
                row_dict['spread'] = result_dec
    return row_dict
    
def parse_spread_cells(cell_list):
    row_dict = {'line': '',
                'spread': '',
                'fav_index': None}

    row_dict = update_spread_dict(cell_list[0], row_dict)
    if row_dict['spread'] != '':
        row_dict['fav_index'] = 0
    
    row_dict = update_spread_dict(cell_list[1], row_dict)
    
    if row_dict['fav_index'] is None:
        if row_dict['spread'] != '' and row_dict['spread'] != '0':
            row_dict['fav_index'] = 1
    
    return row_dict

def parse_spread_row(row):
    td = row.find('td')
    date_time = td.find('span').getText()
    date = date_time.split(' ')[0]
    time = " ".join(date_time.split(' ')[2:])
    teams = [a.getText() for a in td.findAll('a')]
    tdo = row.findAll('td')[2]
    tdbr = tdo.findAll('br')
    cell_content = [x.next_sibling for x in tdbr]
    row_dict = parse_spread_cells(cell_content)
    try:
        favorite = teams[row_dict['fav_index']]
    except:
        favorite = ''

    row_data = [date, time] + teams
    row_data.extend([row_dict['line'], row_dict['spread']])
    row_data.append(favorite)
    return row_data

def scrape_spreads(url="https://www.vegasinsider.com/college-basketball/odds/las-vegas/"):
    game_rows = get_table_rows(url)
    data = [parse_spread_row(x) for x in game_rows]
    date = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    [r.insert(0, date) for r in data]
    columns = ['timestamp', 'game_date', 'game_time', 'team_1', 'team_2',
               'line', 'spread', 'favorite']
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

def tcpalm_section(div):
    headers = div.findAll('div', {'class': 'sdi-so-title'})
    gids = [decode(h['id'].replace('Title_7_', '')) for h in headers]
    tables = div.findAll('table')
    team_info = [tcpalm_header(x) for x in headers]
    scores = [tcpalm_game(x) for x in tables]
    game_info = [t + s + [i] for t, s, i in zip(team_info, scores, gids)]
    return game_info

def game_results(date):
    time.sleep(1)
    timestamp = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

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


def make_link(gid):
    url_b = "http://stats.tcpalm.com/basketball/ncaab-boxscores.aspx?page=/data/NCAAB/results/2019-2020/boxscore"
    url = "".join([url_b, gid, ".html"])
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
    return zip(team_cols, team_totals)    

def game_box(url):
    time.sleep(1)
    try:
        r = requests.get(url)
        bid = re.findall(r'\d+', url.split('/')[-1])[0]
        soup = BeautifulSoup(r.content, 'html.parser')
        divs = soup.findAll('div', {'class': 'sdi-so'})[1:3]
        team_totals = [process_team(div) for div in divs]
        cols = [x[0] for x in team_totals[0]]
        team_dicts = [{k:v for k, v in t} for t in team_totals]
        cols_both = ['away_' + c for c in cols] + ['home_' + c for c in cols]
        team_data = [[td[c] for c in cols] for td in team_dicts]
        game_data = team_data[0] + team_data[1]
        # box id
        bid = re.findall(r'\d+', url.split('/')[-1])[0]
        cols_both.insert(0, 'bid')
        game_data.insert(0, bid)
        return [cols_both, game_data]
    except:
        return [[]]


def game_dict(game_rows):
    gd = {k:v for k, v in zip(game_rows[0], game_rows[1])}
    return gd
    
def data_from_dict(key, game_dict):
    try:
        result = game_dict[key]
    except:
        result = ""
    return result

def date_boxes(box_list):
    # to ensure same shape for box scores missing some statistics
    unique_cols = list(set([col for game in box_list for col in game[0]]))
    unique_cols.sort()
    game_dicts = [game_dict(x) for x in box_list]
    data = [[data_from_dict(k, gd) for k in unique_cols] for gd in game_dicts]
    data.insert(0, unique_cols)
    return data

def game_results(date):
    print 'date %s' % (date)
    time.sleep(1)
    timestamp = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    url = tcpalm_url(date)
    r = requests.get(url)
    soup = BeautifulSoup(r.content, 'html.parser')
        
    try:
        div = soup.find('div', {'class': 'sdi-divScoreColumn_1-2'})
        games = tcpalm_section(div)
        gids = [game[-1] for game in games]
        links = [make_link(g) for g in gids]
        boxes = [game_box(x) for x in links]

        try:
            div2 = soup.find('div', {'class': 'sdi-divScoreColumn_2-2'})
            div2_games = tcpalm_section(div2)
            gids2 = [game[-1] for game in div2_games]
            links2 = [make_link(g) for g in gids2]
            div2_boxes = [game_box(x) for x in links2]
            # first row is col names, not needed
            games.extend(div2_games)
            boxes.extend(div2_boxes)
        except:
            pass
            
        [g.extend([date, timestamp]) for g in games]
    
        box_data = date_boxes(boxes)
        col_names = ['away_team', 'home_team', 'neutral', 'away_score', 'home_score', 'sid',
                     'date', 'timestamp']
        col_names.extend(box_data[0])
        
        # first row of box_data is column names, don't include
        all_data = [g + b for g, b in zip(games, box_data[1:])]
        
        all_data.insert(0, col_names)
        
    except:
        all_data = [['date', 'timestamp'], [date, timestamp]]

    return all_data
