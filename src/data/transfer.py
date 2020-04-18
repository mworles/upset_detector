""" transfer

A module to insert to and extract data from local MySQL database.

Classes
-------
DBColumn
    Define.
DBTable
    Define.
DBAssist
    Define.

Functions
---------

"""
import ConfigParser
import os
import json
import math
import pandas as pd
import numpy as np
import pymysql


class DBColumn():
    """
    A container of data and metadata for an individual column.

    Attributes
    ----------
    data : list
        Column data assigned as input to the class instance. First element is
        str column name.
    name: str
        Name of the column.
    values: list
        List of element values.
    type: str
        String defining the column type for MySQL table.
    query: str
        String containing the column name and type for a create or insert
        query.

    Methods
    -------
    convert_values
        Returns the column values converted to necessary format for insertion
        to MySQL table.
    column_type
        Returns the MySQL column type inferred from the column values.
    format_value
        Define.
    """
    def __init__(self, data):
        self.data = data
        self.name = self.data[0].replace('-', '_')
        self.values = self.data[1:]
        self.type = self.column_type(self.values)
        self.query = "".join([self.name, self.type])

    # static method used when creating class instance
    @staticmethod
    def column_type(col_values):
        """Returns string indicating MYSQL type to use for column.

        Parameters
        ----------
        col_values : list or array
            All data points included in the column.

        Returns
        -------
        col_type : str
            MySQL column type for a CREATE/INSERT query.

        """
        # collect all unique types as strings
        elem_types = list(set([type(x).__name__ for x in col_values]))
        
        # if any strings use VARCHAR, otherwise use FLOAT
        if 'str' in elem_types:
            # keep len at 64, sufficient size for expected values
            col_type = " VARCHAR (64) "
        else:
            col_type = " FLOAT "
        
        return col_type
    
    def convert_values(self):
        """
        Return list of column values converted for MySQL insertion.

        Raw column values of numeric or string types are converted to
        corresponding string values for MySQL insert queries. Raw values of
        None, empty strings, or NaN are converted to NULL. 
        
        Returns
        -------
        new_values : list
            List of column values converted for MySQL insert command.

        """
        new_values = [self.format_value(x, self.type) for x in self.values]
        nulls = ['nan', '', 'None', "'nan'", "''", "'None'"]
        new_values = [x if x not in nulls else 'NULL' for x in new_values]
        return new_values

    @staticmethod
    def format_value(raw_value, col_type):
        """Return element as a formatted string for insertion to MySQL table.

        Parameters
        ----------
        raw_value : str, float, int, or NaN
            The original value provided as input to the DBTable.
        col_type : {'VARCHAR', 'FLOAT'}
            The MySQL type for the column.

        Returns
        -------
        new_value : str
            Value converted to string.

        """
        # code nulls separately to prevent inserting as strings
        if "VARCHAR" in col_type:
            # convert any numerics to string, keep any existing ' intact
            new_value = str(raw_value).replace("'", r"\'")
            new_value = r"""'{}'""".format(new_value)
        else:
            new_value = r"""{}""".format(raw_value)

        return new_value


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
