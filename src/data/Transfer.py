import ConfigParser
import pyodbc
import csv
import Clean
import time


def extract_data(file):
    """Extract and return all rows from data file as list of lists."""
    
    with open(file) as csvfile:
        reader = csv.reader(csvfile)
        rows = [x for x in reader]
    
    len_data = len(rows)
    
    return rows

def file_rows(name, directory):
    """Given file name, return data as list of lists."""
    file = "".join([directory, name, ".csv"])
    rows = extract_data(file)
    return rows

def no_digits(x):
    """Returns false if item contains only digits, otherwise true."""
    no_digits = True
    try:
        y = float(x)
        no_digits = False
    except:
        pass
    return no_digits

def column_type(col):
    """Given a vector of data, returns type of column for MYSQL create table."""
    col_texts = [no_digits(x) for x in col]
    if any(col_texts):
        col_type = """ TEXT """
    else:
        col_type = """ NUMERIC """
    return col_type

def query_column(col):
    """Given a vector of data with header, returns portion of query
    with column name and data type for MSQL create table."""
    col_name = col[0]
    col_type = column_type(col[1:])
    column_details = "".join([col_name, col_type])
    return column_details

def query_columns(column_list):
    column_details = [query_column(x) for x in column_list]
    qcs = ", ".join(column_details)
    return qcs

def query_create_table(name, rows):
    """Returns full MYSQL query needed to create table from flat .csv file."""
    
    # transpose rows to list of columns, to extract column type
    column_list = map(list, zip(*rows))
    
    # get portion of query for columns
    qc = query_columns(column_list)
    
    # combine statements to create full query
    q = " ".join(["""CREATE TABLE""", name, """(""", qc, """);"""])
    
    return q


def format_value(x):
    """Format string value for MYSQL insert statement."""
    if no_digits(x):
        x = x.replace("'", r"\'")
        xf = r"""'%s'""" % (x)
    else:
        xf = """%s""" % (x)
    return xf

def format_rows(rows):
    """Return list of rows formatted for MYSQL insert."""
    # convert list of rows to list of columns
    # to iterate conversion over full column vector
    column_list = map(list, zip(*rows))
    
    def convert_col(col):
        col_new = map(lambda x: format_value(x), col)
        return col_new
    
    cl_new = [convert_col(x) for x in column_list]
    
    rows_f = map(list, zip(*cl_new))
    
    return rows_f


def row_as_query(row):
    """Return row list as string containing portion of MYSQL insert query."""
    row_s = ", ".join(row)
    q_values = "".join(["(", row_s, ")"])
    return q_values

def query_insert_rows(rows):
    """Return full MYSQL insert query for all rows to insert."""
    # list of separate row values for mysql insert
    row_queries = map(lambda x: row_as_query(x), rows)
    # combine lists to form one block
    rows_combined = ",\n".join(row_queries)
    # form full query with table name
    pref = " ".join(["INSERT INTO ", table_name, "VALUES"])
    query_insert = " ".join([pref, rows_combined, ";"])
    
    return query_insert

def insert_rows(table_name, rows, cursor):
    """Given table name with list of rows, insert all rows."""
    # obtain full insert query for all rows
    query_insert = query_insert_rows(rows)
    try:
        result = cursor.execute(query_insert)
        return True
    except:
        return False

def table_exists(table_name, cursor):
    qp = """SELECT COUNT(*) FROM information_schema.tables WHERE table_name = """ 
    qtn = """'%s'""" % (table_name)
    q_full = "".join([qp, qtn])
    cursor.execute(q_full)
    
    result = False
    
    if cursor.fetchone()[0] == 1:
        result = True
    
    return result

def create_and_insert(table_name, directory, cursor):
    # extract data as list of rows
    print 'extracting table: %s' % (table_name)
    rows = file_rows(table_name, directory)
    # first row is column headers
    print '%s rows extracted' % (len(rows) - 1)
    
    if table_exists(table_name, cursor) == False:
        q_create = query_create_table(table_name, rows)
        cursor.execute(q_create)
    
    rows_data = format_rows(rows[1:])
    result = insert_rows(table_name, rows_data, cursor)
    

def transfer_directory(directory, cursor):
    
    file_names = data.Clean.list_of_filenames(directory)
    
    l_tables = [create_and_insert(x, directory, cursor) for x in file_names]
    
    return l_tables


config = ConfigParser.ConfigParser()
config.readfp(open('../../aws.config'))
server = config.get('RDS', 'server')
database = config.get('RDS', 'database')
uid = config.get('RDS', 'uid')
code = config.get('RDS', 'code')

# establish connection to database and create cursor
conn = pyodbc.connect('DRIVER=MySQL ODBC 8.0 ANSI Driver;'
                      'SERVER='+server+';'
                      'DATABASE='+database+';'
                      'UID='+uid+';'
                      'PWD='+code+';'
                      'charset=utf8mb4;')
cursor = conn.cursor()

# create tables and insert
l_tables = transfer_directory(datdir, cursor)

# commit changes and close connection
conn.commit()
conn.close()
