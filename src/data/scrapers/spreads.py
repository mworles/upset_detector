import utils
import re

def update_spread_dict(x, row_dict):
    regex = "\d{3}"
    try:
        line = re.findall(regex, x)[0]
        row_dict['line'] = line
    except:
        result_enc = x.encode('utf-8').strip()

        if 'PK' in result_enc:
            row_dict['spread'] = '0'
        else:
            result_dec = result_enc.decode('ascii', 'ignore')
            #result_split = result_dec.split('-')
            try:
                result = result_dec.split('-')[1]
                uni_5 = 'xc2\\xbd\\xc2\\xa0'
                if uni_5 in result_enc.encode('string_escape'):
                    result = result + '.5'
                else:
                    result = result + '.0'
                row_dict['spread'] = result
            except:
                row_dict['spread'] = result_dec
    return row_dict

def parse_spread_cells(cell_list):
    row_dict = {'line': '',
                'spread': '',
                'fav_index': None}

    row_dict = update_spread_dict(cell_list[0], row_dict)
    if row_dict['spread'] != '':
        row_dict['fav_index'] = 0
    
    row_dict = update_spread_dict(cell_list[1], row_dict)
    
    if row_dict['fav_index'] is None:
        if row_dict['spread'] != '' and row_dict['spread'] != '0':
            row_dict['fav_index'] = 1
    
    return row_dict

def parse_spread_row(row):
    td = row.find('td')
    date_time = td.find('span').getText()
    date = date_time.split(' ')[0]
    time = " ".join(date_time.split(' ')[2:])
    teams = [a.getText() for a in td.findAll('a')]
    tdo = row.findAll('td')[2]
    tdbr = tdo.findAll('br')
    cell_content = [x.next_sibling for x in tdbr]
    row_dict = parse_spread_cells(cell_content)
    try:
        favorite = teams[row_dict['fav_index']]
    except:
        favorite = ''

    row_data = [date, time] + teams
    row_data.extend([row_dict['line'], row_dict['spread']])
    row_data.append(favorite)
    return row_data

def run():
    url="https://www.vegasinsider.com/college-basketball/odds/las-vegas/"
    game_rows = utils.get_table_rows(url)
    data = [parse_spread_row(x) for x in game_rows]
    date = utils.current_timestamp()
    [r.insert(0, date) for r in data]
    columns = ['timestamp', 'game_date', 'game_time', 'team_1', 'team_2',
               'line', 'spread', 'favorite']
    data.insert(0, columns)
    return data
