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
    A container for a column of data in DBTable.

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
        String containing the column name and type for CREATE/INSERT query.
    
    Methods
    -------
    column_type
        Return the MySQL column type inferred from the column values.
    convert_column
        Return the column values converted to formatted strings.
    convert_element
        Return one element converted to a formatted string.
    """
    def __init__(self, data):
        """Initialize DBColumn instance."""
        self.data = data
        self.name = self.data[0].replace('-', '_')
        self.values = self.data[1:]
        self.type = self.column_type()
        self.query = "".join([self.name, self.type])

    def column_type(self):
        """Return string indicating MYSQL type to use for column."""
        # collect all unique types as strings
        elem_types = list(set([type(x).__name__ for x in self.values]))

        # if any strings use VARCHAR, otherwise use FLOAT
        if 'str' in elem_types:
            # keep len at 64, sufficient size for expected values
            col_type = " VARCHAR (64)"
        else:
            col_type = " FLOAT"

        return col_type

    def convert_column(self):
        """
        Return list of column values converted for MySQL insertion.
    
        Returns
        -------
        new_values : list of str
            String values converted from numeric or string types. None, 
            empty strings, or NaN are converted to NULL.
        
        """
        new_values = [self.convert_element(x) for x in self.values]
        nulls = ['nan', '', 'None', "'nan'", "''", "'None'"]
        new_values = [x if x not in nulls else 'NULL' for x in new_values]
        return new_values

    def convert_element(self, raw_value):
        """
        Return element as a formatted string for insertion to MySQL table.

        Parameters
        ----------
        raw_value : str, float, int, NaN, or None
            The original cell value when DBTable is initialized.

        Returns
        -------
        new_value : str
            Value converted to string.

        """
        # code nulls separately to prevent inserting as strings
        if "VARCHAR" in self.type:
            # convert any numerics to string, keep any existing ' intact
            new_value = str(raw_value).replace("'", r"\'")
            new_value = r"""'{}'""".format(new_value)
        else:
            new_value = """{}""".format(raw_value)

        return new_value


class DBTable():
    """
    A container for 2-dimensional data for insertion to MySQL table.

    Attributes
    ----------
    name : str
        Name of the table in MySQL database.
    data : list of list
        First list must be column names, remaining lists contain rows of data.
    columns: list of DBColumn
        Contains DBColumn instance for each column in the table.

    Methods
    -------
    query_rows
        Return table rows as list of formatted strings.    
    query_create
        Return query for creating table in MySQL database.
    query_insert
        Return query for inserting data to table in MySQL database.
    """
    def __init__(self, name, data):
        """Initialize DBTable instance."""
        self.name = name
        self.data = data
        # initialize all columns to get names, types, and values
        self.columns = [DBColumn(x) for x in map(list, zip(*self.data))]

    def query_create(self):
        """Return query for creating table in MySQL database."""
        query_columns = ", \n".join([c.query for c in self.columns])
        pref = "CREATE TABLE IF NOT EXISTS"
        #create = """ """.join([pref, self.name, "(", query_columns, ");"])
        create = "{0} {1} ({2});".format(pref, self.name, query_columns)
        return create

    def query_insert(self, at_once=True):
        """
        Return query for inserting data to table in MySQL database.
        
        Parameters
        ----------
        at_once : bool, default True
            Return a single formatted string for inserting all rows at once.
            If False, return a list of separate strings for each row.

        Returns
        -------
        insert : str or list of str
            Str or list for inserting table values to MySQL database.

        """
        # create prefix string for insert query
        col_string = ", ".join(self.data[0])
        pref = "INSERT INTO {0} ({1}) VALUES".format(self.name, col_string)
        
        # list with formatted string for each row
        table_rows = self.query_rows()
        
        if at_once is True:
            rows_combined = ",\n".join(table_rows)
            insert = "{0} {1};".format(pref, rows_combined)
        else:
            insert = ["{0} {1};".format(pref, row) for row in table_rows]

        return insert

    def query_rows(self):
        """Return table rows as list of formatted strings."""
        column_values = [c.convert_column() for c in self.columns]
        rows = map(list, zip(*column_values))
        row_strings = [", ".join(row_list) for row_list in rows]
        rows_formatted = ["({0})".format(row) for row in row_strings]
        return rows_formatted


class DBAssist():
    """
    A tool for inserting data to and extracting data from MySQL database.

    Attributes
    ----------
    conn : pymysql.connection
        An instance of pymysql connection class to establish connection with
        MySQL database.
    cursor : pymysql.cursor
        Instance of pymysql cursor class called from the conn attribute.

    Methods
    -------
    query_rows
        Return table rows as list of formatted strings.    
    query_create
        Return query for creating table in MySQL database.
    query_insert
        Return query for inserting data to table in MySQL database.
    """
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
        create = table.query_create()
        self.cursor.execute(create)
        self.conn.commit()

    def execute_commit(self, query):
        self.cursor.execute(query)
        self.conn.commit()
        
    def insert_rows(self, table, at_once=True):
        """Given table name with list of rows, insert all rows."""
        # obtain full insert query for all rows
        if at_once == True:
            insert_full = table.query_insert(at_once=at_once)
            self.cursor.execute(insert_full)
            self.conn.commit()
        else:
            insert_rows = table.query_insert(at_once=at_once)
            map(lambda x: self.execute_commit(x), insert_rows)

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
