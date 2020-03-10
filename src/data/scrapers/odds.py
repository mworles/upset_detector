import unicodedata
import utils

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

def run():
    url="https://www.vegasinsider.com/college-basketball/odds/las-vegas/money/"
    game_rows = utils.get_table_rows(url)
    data = [parse_odds_row(row) for row in game_rows]
    timestamp = utils.current_timestamp()
    [r.insert(0, timestamp) for r in data]
    columns = ['timestamp', 'game_date', 'game_time', 'team_1', 'team_2',
               'odds1', 'odds2']
    data.insert(0, columns)
    return data
