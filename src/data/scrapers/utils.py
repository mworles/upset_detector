from bs4 import BeautifulSoup
import requests
import datetime
from selenium import webdriver

def get_table_rows(url):
    r = requests.get(url)
    soup = BeautifulSoup(r.content, 'html.parser')
    table = soup.find('table', {'class': 'frodds-data-tbl'})
    table_rows = table.findAll('tr')
    game_rows = [x for x in table_rows if len(x.findAll('td')) > 1]
    return game_rows

def current_timestamp():
    date = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    return date

def get_soup(url):
    f = requests.get(url)
    soup = BeautifulSoup(f.text, features='lxml')
    return soup

def set_browser(driver_path = 'driver/chromedriver.exe'):
    browser = webdriver.Chrome(driver_path)
    return browser
