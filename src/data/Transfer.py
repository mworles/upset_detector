import ConfigParser
import csv
import clean
import time
import pymysql
import os
import json
import pandas as pd
import math

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
            col_f = [x for x in col if x is not None]
            col_f = [str(float(x)) if x != '' else 'NULL' for x in col_f]
            # only non-null values for decimal format extraction
            col_f = [x for x in col_f if x != 'NULL']
            col_f = [x for x in col_f if math.isnan(float(x)) == False]
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
        if x == None:
            x = 'NULL'
        try:
            if math.isnan(x) == True:
                x = 'NULL'
        except:
            if x == '':
                x = 'NULL'

        if "VARCHAR" in col_type:
            if x != 'NULL':
                x = str(x)
                x = x.replace("'", r"\'")
                xf = r"""'%s'""" % (x)
            else:
                xf = r"""NULL"""
        elif "DECIMAL" in col_type:
            if x == 'NULL':
                xf = """NULL"""
            else:
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

    def get_row_values(self):
        values_conv = [c.convert_values() for c in self.columns]
        rows_conv = map(list, zip(*values_conv))
        rows_joined = [", ".join(r) for r in rows_conv]
        self.row_values = ["".join(["(", r, ")"]) for r in rows_joined]
        
    def get_query_insert(self):
        rows_combined = ",\n".join(self.row_values)
        col_pref = ", ".join(self.column_names)
        col_pref = "".join(['(', col_pref, ')'])
        pref = " ".join(["INSERT INTO ", self.name, col_pref, "VALUES"])
        self.query_insert = " ".join([pref, rows_combined, ";"])
        self.query_rows = [pref + r for r in self.row_values]
        
    def setup_table(self):
        self.setup_columns()
        self.get_query_create()
        self.get_row_values()
        self.get_query_insert()


class DBAssist():
    def __init__(self):
        self.conn = None
        self.cursor = None

    def connect(self):
        parser = ConfigParser.ConfigParser()
        config_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), '../../.config')
        parser.readfp(open(config_path))
        pwd = parser.get('Local', 'pwd')
        db = parser.get('Local', 'db')

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
        self.cursor.execute(table.query_create)
        self.conn.commit()

    def execute_commit(self, query):
        self.cursor.execute(query)
        self.conn.commit()
        
    def insert_rows(self, table, at_once=True):
        """Given table name with list of rows, insert all rows."""
        # obtain full insert query for all rows
        if at_once == True:
            self.cursor.execute(table.query_insert)
            self.conn.commit()
        else:
            map(lambda x: self.execute_commit(x), table.query_rows)

    def run_query(self, query):
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        return result
    
    def table_schema(self, table_name):
        self.cursor = self.conn.cursor(pymysql.cursors.DictCursor)
        query_cols = """SHOW COLUMNS FROM %s;""" % (table_name)
        result = self.run_query(query_cols)
        return result
        
    def table_rows(self, result, schema):
        columns = [r['Field'] for r in schema]
        column_types = [r['Type'] for r in schema]
        i_dec = [schema.index(c) for c in schema if 'decimal' in c['Type']]
        rows_raw = [[r[c] for c in columns] for r in result]
        if len(rows_raw) == 0:
            table = [[]]
        else:
            data_by_cols = map(list, zip(*rows_raw))
            for i in i_dec:
                data_by_cols[i] = [float(x) if x is not None else None for x in data_by_cols[i]]
            rows = map(list, zip(*data_by_cols))
            table = [columns] + rows
        return table
        
    def return_table(self, table_name, modifier=None):
        schema = self.table_schema(table_name)
        # list of column indices where type is decimal
        query_rows = """SELECT * FROM %s""" % (table_name)
        if modifier is not None:
            query_rows += ' ' + modifier
        query_rows += """;"""
        result = self.run_query(query_rows)
        table = self.table_rows(result, schema)
        return table
    
    def return_df(self, table_name, modifier=None):
        table = self.return_table(table_name, modifier=modifier)
        df = pd.DataFrame(table[1:], columns=table[0])
        return df

    def close(self):
        self.conn.close()

def dataframe_rows(df):
    """Return 2-d df as list of lists, first list is column names."""
    col_names = list(df.columns)
    rows = df.values.tolist()
    rows.insert(0, col_names)
    return rows

def insert(name, rows, at_once=True, create=False, delete=False):
    dbt = DBTable(name, rows)
    dbt.setup_table()
    dba = DBAssist()
    dba.connect()
    if create == True:
        dba.create_table(dbt)
    if delete == True:
        query = "DELETE FROM %s" % (name)
        dba.run_query(query)
    dba.insert_rows(dbt, at_once=at_once)
    dba.close()
    
def insert_df(name, df, at_once=True, create=False, delete=False):
    rows = dataframe_rows(df)
    insert(name, rows, at_once=at_once, create=create, delete=delete)

def query_from_schema(table_name, schema_file):
    with open(schema_file, 'r') as f:
        schema = json.load(f)[table_name]
    query_create = """CREATE TABLE """ + table_name
    cols = [str(" ".join([c['name'], c['type']])) for c in schema]
    cols_one = ", \n".join(cols)
    query_create = " ".join([query_create, "(", cols_one, ");"])
    return query_create

def create_from_schema(table_name, schema_file):
    query = query_from_schema(table_name, schema_file)
    dba = DBAssist()
    dba.connect()
    dba.cursor.execute(query)
    dba.conn.commit()
    dba.close()    

def return_data(table_name, modifier=None):
    dba = DBAssist()
    dba.connect()
    df = dba.return_df(table_name, modifier=modifier)    
    dba.close()
    return df
