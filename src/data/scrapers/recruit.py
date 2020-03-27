from bs4 import BeautifulSoup
import requests
import time
import re

def process_row(li):
    name = li.find('div', {'class': 'recruit'}).find('a').getText()
    rating = li.find('div', {'class': 'rating'}).find('div')
    star = len(rating.findAll('span', {'class': 'icon-starsolid yellow'}))
    score = rating.find('span', {'class': 'score'}).getText()
    status = li.find('div', {'class': 'status'})
    stat_link = status.find('a')['href']
    college_pre = 'https://247sports.com/college/'
    if college_pre in stat_link:
        stat_link = stat_link.replace('https://247sports.com/college/', '')
        school = stat_link.split('/')[0]
    else:
        school = 'professional'
    try:
        return [str(name), star, float(score), str(school)]
    except:
        return []

def set_from_soup(soup, n_collected):
    div = soup.find('div', {'class': 'rankings-page__main'})
    lis = div.find_all('li', {'class': 'rankings-page__list-item'})
    all_rows = lis[:-1]
    set_rows = all_rows[n_collected:]
    set_players = map(process_row, set_rows)
    return set_players

def season_url(season):
    pref = 'https://247sports.com/Season/'
    suf = '-Basketball/CompositeRecruitRankings/?InstitutionGroup=HighSchool'
    url = ''.join([pref, str(season), suf])
    return url

def number_of_sets(soup):
    header = soup.find('h1', {'class': 'rankings-page__heading'})
    n_total = header.find('span').getText()
    n_total = re.sub("[\s()]", "", n_total)
    n_sets = (int(n_total) / 50) + 1
    return n_sets 

def players_from_season(season, browser):
    browser.get(season_url(season))
    time.sleep(4)
    soup = BeautifulSoup(browser.page_source, 'html.parser')
    n_sets = number_of_sets(soup)

    n_collected = 0
    players = []
    n_set = 0

    while n_set < n_sets:
        set_players = set_from_soup(soup, n_collected)
        players.extend(set_players)
        n_collected += len(set_players)
        n_set += 1

        browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        
        try:
            load = browser.find_element_by_link_text('Load More')
            load.click()
            time.sleep(2)
            soup = BeautifulSoup(browser.page_source, 'html.parser')
        except:
            pass
    
    browser.quit()

    ranks = range(1, len(players) + 1)
    players = [x for x in players if len(x) != 0]
    data = [[season, rank] + row for rank, row in zip(ranks, players)]
    col_names = ['season', 'ranking', 'name', 'star', 'rating', 'school']
    data.insert(0, col_names)
    
    return data
