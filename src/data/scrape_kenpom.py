from bs4 import BeautifulSoup
import requests
import datetime
import Transfer

# Create a method that parses a given year and spits out a raw dataframe
def get_ratings(url):
    """
    Imports raw data from kenpom website.
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
    rows = [[x.getText() for x in tdl] for tdl in tds]
    
    rows.insert(0, names)
    
    return rows

url = 'http://kenpom.com/index.php'
rows = get_ratings(url)
rows[0].insert(0, 'date')

date = str(datetime.datetime.now().date())

[x.insert(0, date) for x in rows[1:]]
