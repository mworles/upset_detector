import requests
from bs4 import BeautifulSoup
import pandas as pd
import datetime
import unicodedata
import sys
sys.path.append("../")
from Cleaning import write_file


def clean_string(x):
    x = unicodedata.normalize("NFKD", x)
    x = x.encode('ascii', 'ignore').strip()
    return x

def parse_row(row):
    td = row.find('td')
    date_time = td.find('span').getText()
    date = date_time.split(' ')[0]
    time = " ".join(date_time.split(' ')[2:])
    teams = [a.getText() for a in td.findAll('a')]
    tdo = row.findAll('td')[2]
    #tdo = row.findAll('td')[8]
    tdbr = tdo.findAll('br')
    ml = [clean_string(x.next_sibling) for x in tdbr]
    row_data = [date, time] + teams + ml
    return row_data

def get_odds_rows(url):
    print "Scraping {}...".format(url)
    r = requests.get(url)
    soup = BeautifulSoup(r.content, 'html.parser')
    table = soup.find('table', {'class': 'frodds-data-tbl'})
    table_rows = table.findAll('tr')
    game_rows = [x for x in table_rows if len(x.findAll('td')) > 1]
    return game_rows


url = "http://www.vegasinsider.com/college-basketball/odds/las-vegas/money/"

game_rows = get_odds_rows(url)
data = [parse_row(row) for row in game_rows]
now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
now = now.translate(None, ''.join([':'])).replace('-', '_').replace(' ', '_')
columns = ['date', 'time', 'team_1', 'team_2', 'odds1', 'odds2']
df = pd.DataFrame(data, columns = columns)
df = df.sort_values(['date', 'time'])

data_out = "../../data/external/vi/"
#write_csv(df, data_out, file_name, keep_index=False)
write_file(df, data_out, now)
