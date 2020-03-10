import utils

def no_span(td):
    has_span = td.find('span')
    if has_span is None:
        return td.getText()
    else:
        return None

def clean_text(x):
    x = x.replace('+', '')
    return x

def run():
    """
    Reads team ratings data from kenpom website.
    """
    url = 'http://kenpom.com/index.php'
    soup = utils.get_soup(url)
    t = soup.find('table', {'id': 'ratings-table'})
    thead = t.find('thead')
    thr_list = thead.find_all('tr')[-1].find_all('th')
    names = [x.getText() for x in thr_list]
    names = [x.replace('-', '_') for x in names]
    
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


    rows = [[no_span(td) for td in row] for row in tds]
    rows = [[x for x in row if x is not None] for row in rows]
    rows = [[clean_text(x) for x in row] for row in rows]
    
    timestamp = utils.current_timestamp()
    [r.insert(0, timestamp) for r in rows]
    names.insert(0, 'date')
    rows.insert(0, names)

    return rows
