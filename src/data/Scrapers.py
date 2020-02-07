from bs4 import BeautifulSoup
import requests
import datetime
import requests
import unicodedata


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

def scrape_odds(url):
    game_rows = get_table_rows(url)
    data = [parse_odds_row(row) for row in game_rows]
    date = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    [r.insert(0, date) for r in data]
    columns = ['timestamp', 'game_date', 'game_time', 'team_1', 'team_2', 'odds1', 'odds2']
    data.insert(0, columns)
    return data


def encode_spread_cell(x):
    x = x.encode('utf-8')
    return x
    
def get_line(line_spread):
    line = [x for x in line_spread if 'u' in x][0]
    line = line.split('u')[0]
    return line

def get_spread(line_spread):
    spread = [x for x in line_spread if 'u' not in x][0]
    spread_i = line_spread.index(spread)
    spread = spread.split('-')[1]
    return spread, spread_i

def spread_decimal(spread):
    decimal_uni = "".join(spread.encode('string_escape').split('\\')[1:])
    unicode_dict = {'xc2xbdxc2xa0': '5', 'xc2xa0': '0'}
    decimal = unicode_dict[decimal_uni]
    return decimal

def parse_spread_row(row):
    td = row.find('td')
    date_time = td.find('span').getText()
    date = date_time.split(' ')[0]
    time = " ".join(date_time.split(' ')[2:])
    teams = [a.getText() for a in td.findAll('a')]
    tdo = row.findAll('td')[2]
    tdbr = tdo.findAll('br')
    line_spread = [encode_spread_cell(x.next_sibling) for x in tdbr]
    line = get_line(line_spread).decode('ascii', 'ignore')
    spread_uni, spread_i = get_spread(line_spread)
    decimal = spread_decimal(spread_uni)
    spread = spread_uni.decode('ascii', 'ignore')
    spread = "".join([spread, '.', decimal])
    favorite = teams[spread_i]
    row_data = [date, time] + teams
    row_data.extend([line, spread, favorite])
    return row_data

def scrape_spreads(url):
    game_rows = get_table_rows(url)
    data = [parse_spread_row(x) for x in game_rows]
    date = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    [r.insert(0, date) for r in data]
    columns = ['timestamp', 'game_date', 'game_time', 'team_1', 'team_2',
               'line', 'spread', 'favorite']
    data.insert(0, columns)
    return data
