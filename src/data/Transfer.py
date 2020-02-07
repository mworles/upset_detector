import ConfigParser
import pyodbc
import csv
import Clean
import time
import pymysql
import os

def rows_from_file(file):
    """Extract and return all rows from data file as list of lists."""
    
    with open(file) as csvfile:
        reader = csv.reader(csvfile)
        rows = [x for x in reader]
    
    return rows

def extract_data(name, directory):
    """Given file name, return data as list of lists."""
    file = "".join([directory, name, ".csv"])
    rows = rows_from_file(file)
    return rows

def transfer_directory(directory, cursor):
    
    file_names = data.Clean.list_of_filenames(directory)
    
    l_tables = [create_and_insert(x, directory, cursor) for x in file_names]
    
    return l_tables


class DBColumn():
    
    def __init__(self, data):
        self.data = data
        self.name = self.clean_column_name(self.data[0])
        self.values = self.data[1:]
        self.type = self.get_column_type(self.values)
        self.query = "".join([self.name, self.type])

    def convert_values(self):
        values_conv = map(lambda x: self.format_value(x, self.type), self.values)
        return values_conv

    @staticmethod
    def clean_column_name(name):
        name_clean = name.replace('-', '_')
        return name_clean
    
    @staticmethod
    def get_column_type(col):
        try:
            col_f = [str(float(x)) for x in col]
            dec_splits = [x.split('.') for x in col_f]
            imax = max([len(x[0]) for x in dec_splits])
            dmax = max([len(x[1]) for x in dec_splits])
            # for DECIMAL (M,D) mysql requires M >= D
            col_type = """ DECIMAL (%s, %s) """ % (imax + dmax, dmax)
        except:
            col_type = """ VARCHAR (64) """
    
        return col_type

    @staticmethod
    def format_value(x, col_type):
        """Format string value for MYSQL insert statement."""
        if "VARCHAR" in col_type:
            x = x.replace("'", r"\'")
            x = x.replace(".", "")
            xf = r"""'%s'""" % (x)
        elif "DECIMAL" in col_type:
            xf = """%s""" % (x)
        return xf
        
class DBTable():

    def __init__(self, name, data):
        self.name = name
        self.column_names = data[0]
        self.column_list = map(list, zip(*data))
    
    def setup_columns(self):
        self.columns = [DBColumn(x) for x in self.column_list]
        
    def get_query_create(self):
        q_columns = ", \n".join([c.query for c in self.columns])
        q_create = """ """.join(["CREATE TABLE IF NOT EXISTS", self.name, "(", q_columns, ");"])
        self.query_create = q_create

    def get_query_rows(self):
        values_conv = [c.convert_values() for c in self.columns]
        rows_conv = map(list, zip(*values_conv))
        rows_joined = [", ".join(r) for r in rows_conv]
        rows_queries = ["".join(["(", r, ")"]) for r in rows_joined]
        rows_combined = ",\n".join(rows_queries)
        return rows_combined
        
    def get_query_insert(self):
        query_rows = self.get_query_rows()
        pref = " ".join(["INSERT INTO ", self.name, "VALUES"])
        self.query_insert = " ".join([pref, query_rows, ";"])
        
    def setup_table(self):
        self.setup_columns()
        self.get_query_create()
        self.get_query_insert()


class DBAssist():
    def __init__(self):
        self.conn = None
        self.cursor = None

    def connect(self):
        parser = ConfigParser.ConfigParser()
        config_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), '../../.config')
        parser.readfp(open(config_path))
        driver = parser.get('RDS', 'driver')
        server = parser.get('RDS', 'server')
        database = parser.get('RDS', 'database')
        uid = parser.get('RDS', 'uid')
        code = parser.get('RDS', 'code')
        pwd = parser.get('Local', 'pwd')
        db = parser.get('Local', 'db')
        """
        self.conn = pyodbc.connect('DRIVER='+driver+';'
                                   'SERVER='+server+';'
                                   'DATABASE='+database+';'
                                   'UID='+uid+';'
                                   'PWD='+code+';'
                                   'charset=utf8mb4;')
        """
        self.conn = pymysql.connect(host='127.0.0.1',
                                    port=3306,
                                    user='root',
                                    passwd=pwd,
                                    db=db)
        self.cursor = self.conn.cursor()


    def check_table(self, table):
        qp = """SELECT COUNT(*)
                FROM information_schema.tables 
                WHERE table_name = """ 
        tn = """'%s'""" % (table.name)
        q_full = "".join([qp, tn])
        self.cursor.execute(q_full)
        
        if self.cursor.fetchone()[0] == 1:
            result = True
        else:
            result = False
        
        return result

    def create_table(self, table):
        #table_exists = self.check_table(table)
        #if table_exists == False:
        self.cursor.execute(table.query_create)
        self.conn.commit()

    def insert_rows(self, table):
        """Given table name with list of rows, insert all rows."""
        # obtain full insert query for all rows
        self.cursor.execute(table.query_insert)
        self.conn.commit()

    def run_query(self, query):
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        return result

    def close(self):
        self.conn.close()

def scrape_insert(scraper, url, name):
    rows = scraper(url)
    dbt = DBTable(name, rows)
    dbt.setup_table()
    dba = DBAssist()
    dba.connect()
    dba.create_table(dbt)
    dba.insert_rows(dbt)
    dba.close()
