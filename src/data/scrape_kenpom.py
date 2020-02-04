from bs4 import BeautifulSoup
import requests
import datetime
import Transfer

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
    
    date = str(datetime.datetime.now().date())
    [r.insert(0, date) for r in rows]
    
    names.insert(0, 'date')
    
    rows.insert(0, names)

    return rows

url = 'http://kenpom.com/index.php'
rows = get_ratings(url)

dbt = Transfer.DBTable('ratings', rows)



#dba = Transfer.DBAssist()
#dba.connect('../../aws.config')
#dba.create_table('ratings', rows)
#dba.insert_rows('ratings', rows[:10])
