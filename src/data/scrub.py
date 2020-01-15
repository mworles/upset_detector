import pandas as pd
from Clean import write_file

def scrub_file(name, file_map):
    """Convert file using map of new column and file names."""
    file = '../data/raw/' + name + '.csv'
    df = pd.read_csv(file)
    df = df.rename(columns=file_map[name]['columns'])
    df.columns = df.columns.str.lower()
    return df

def scrub_write(name, file_map):
    df = scrub_file(name, file_map)
    data_out = '../data/scrub/'
    name_new = file_map[name]['new_name']
    write_file(df, data_out, name_new, keep_index=False)

def scrub_files(file_map):
    files = file_map.keys()
    for f in files:
        scrub_write(f, file_map)
