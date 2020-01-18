import pandas as pd
import os
import time
import json
import re
from selenium import webdriver
from bs4 import BeautifulSoup
from urllib import urlopen

def parse_row(x):
    row_classes = x['class']
    if 'nob-border' in row_classes:
        row_text = x.find('th').get_text()
        global dt
        dt = row_text.split(' ')[0:3]
        dt.insert(0, dt.pop(1))
        dt = "/".join(dt)
        return "NA"
    elif "deactivate" in row_classes:
        row_cells = x.find_all('td')
        row_text = [c.get_text() for c in row_cells]
        team_text = row_text[1].split(' - ')
        row_data = [dt] + team_text + [row_text[3], row_text[4]]
        return row_data
    else:
        return "NA"

def file_name(year):
    datdir = '../../data/external/odds/'
    year_file = "".join([datdir, "odds", "_", str(year), ".json"])
    return year_file

def store_data(data, year, page):
    year_file = file_name(year)
    
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


def make_url(year):
    urlb = 'https://www.oddsportal.com/basketball/usa/ncaa-'
    suf = '/results/'
    year_string = "".join([str(year - 1), '-', str(year)])
    url = urlb + year_string + suf
    return url

def get_table(soup):
    t = soup.find('table', {'class': ' table-main'})
    rows = [x for x in t.find_all('tr')]
    data = map(lambda x: parse_row(x), rows)
    data = [x for x in data if x is not "NA"]
    return data

def scrape_odds(year):
    all_data = []
    browser = webdriver.Chrome('driver/chromedriver.exe')
    url = make_url(year)
    #html = urlopen(url)
    browser.get(url)
    print 'scraping ' + url
    
    time.sleep(4)
    
    soup = BeautifulSoup(browser.page_source, 'html.parser')

    pagin = soup.find('div', {'id': 'pagination'}).find_all('a')
    lh = pagin[-1]['href']
    lp = int(re.findall(r'\d+', lh)[0])
    
    page = 1
    data = get_table(soup)
    store_data(data, year, page)
    all_data.extend(data)
    
    need_pages = range(2, lp + 1)
    
    for n in need_pages:
        try:
            nav = browser.find_element_by_link_text(str(n))
            nav.click()
            time.sleep(4)
            print year, n
            soup = BeautifulSoup(browser.page_source, 'html.parser')
            data = get_table(soup)
            store_data(data, year, n)
            all_data.extend(data)
        except:
            pass
    
    browser.quit()
    return all_data

all_y = []

for year in range(2009, 2019):
    x = scrape_odds(year)
    all_y.append(x)
