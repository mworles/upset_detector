import unicodedata
import json
import re
import time
import utils
from bs4 import BeautifulSoup
import requests

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
