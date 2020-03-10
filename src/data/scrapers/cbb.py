from bs4 import BeautifulSoup
import requests
import unicodedata
import time

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

def get_schedules(season):
    links = Scrapers.srcbb_schedule_links(season)
    links_got = []
    
    for url in links:
        team = Scrapers.team_from_url(url)
        try:
            sched = Scrapers.srcbb_schedule(url)
            sched = Scrapers.add_season_team(sched, season, team)
            Transfer.insert('cbb_schedule', sched, at_once=True)
            links_got.append(url)
        except:
            print url
            sched = [['season', 'team'], [season, team]]
            Transfer.insert('cbb_schedule_error', sched, at_once=True)
    
    print 'inserted %s rows season %s' % (len(links_got), season)
